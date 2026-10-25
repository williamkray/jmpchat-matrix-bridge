package connector

import (
	"encoding/xml"
	"strings"
)

// photoKind is the tri-state of the XEP-0153 avatar advertisement in a presence stanza.
type photoKind int

const (
	// photoUnknown: no vcard-temp:x:update element, or one without a usable <photo> (XEP-0153
	// "not ready", or a malformed hash). Never acted on; never removes an avatar.
	photoUnknown photoKind = iota
	// photoNone: <photo/> (empty): the sender has no avatar.
	photoNone
	// photoHash: <photo> holds a 40-char hex SHA-1.
	photoHash
)

func (k photoKind) String() string {
	switch k {
	case photoNone:
		return "none"
	case photoHash:
		return "hash"
	}
	return "unknown"
}

// presenceInfo is the result of one token pass over a presence stanza (see parsePresence).
type presenceInfo struct {
	conflict       bool      // 409 / <conflict/> error (nick conflict on join)
	errType        string    // type attribute of the <error> child (cancel, wait, auth, modify), never its text
	errCondition   string    // first stanza-error condition (RFC 6120 namespace, not <text>) inside <error>
	isMUC          bool      // a muc#user element was seen
	sawVCardUpdate bool      // a vcard-temp:x:update element was seen
	rawPhoto       string    // trimmed text of <photo> (1:1 path input, unchanged behaviour)
	photo          photoKind // tri-state, see photoKind
	hash           string    // lowercased 40-hex hash when photo == photoHash
	itemJID        string    // bare real JID from muc#user item@jid, may be empty (never logged at info)
	newNick        string    // item@nick (nick change, status 303)
	statusCodes    []string  // muc#user status codes (110, 303, ...)
	hasOccupantID  bool      // XEP-0421 occupant-id element present
}

// tokenReader is the subset of a token stream parsePresence needs.
type tokenReader interface {
	Token() (xml.Token, error)
}

// parsePresence consumes the children of a presence stanza whose start element was already read
// (depth starts at 1) in a single pass and extracts the signals handlePresence needs. It does no
// I/O, so it is safe on the XMPP Serve goroutine.
func parsePresence(t tokenReader) presenceInfo {
	var info presenceInfo
	var inVCardUpdate, inPhoto, sawPhotoElem bool
	var photoText strings.Builder
	depth := 1
	inError := false // inside the <error> direct child of the presence
	for depth > 0 {
		tok, err := t.Token()
		if err != nil {
			break
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			if depth == 3 && inError && v.Name.Space == stanzaErrorNS && v.Name.Local != "text" && info.errCondition == "" {
				info.errCondition = v.Name.Local
			}
			if depth == 2 && v.Name.Local == "error" {
				inError = true
				info.errType = attrValue(v.Attr, "type")
			}
			if v.Name.Local == "error" {
				for _, a := range v.Attr {
					if a.Name.Local == "code" && a.Value == "409" {
						info.conflict = true
						break
					}
				}
			}
			if v.Name.Local == "conflict" {
				info.conflict = true
			}
			if v.Name.Space == mucUserNS {
				info.isMUC = true
				switch v.Name.Local {
				case "item":
					for _, a := range v.Attr {
						switch a.Name.Local {
						case "jid":
							if a.Value != "" {
								info.itemJID = bareOf(a.Value)
							}
						case "nick":
							info.newNick = a.Value
						}
					}
				case "status":
					for _, a := range v.Attr {
						if a.Name.Local == "code" && a.Value != "" {
							info.statusCodes = append(info.statusCodes, a.Value)
						}
					}
				}
			}
			if v.Name.Space == occupantIDNS && v.Name.Local == "occupant-id" {
				info.hasOccupantID = true
			}
			if v.Name.Local == "x" && v.Name.Space == vcardUpdateNS {
				info.sawVCardUpdate = true
				inVCardUpdate = true
			}
			if inVCardUpdate && v.Name.Local == "photo" {
				inPhoto = true
				sawPhotoElem = true
			}
		case xml.CharData:
			if inPhoto {
				photoText.Write(v)
			}
		case xml.EndElement:
			if depth == 2 && v.Name.Local == "error" {
				inError = false
			}
			depth--
			if v.Name.Local == "photo" {
				inPhoto = false
			}
			if v.Name.Local == "x" && v.Name.Space == vcardUpdateNS {
				inVCardUpdate = false
			}
		}
	}
	info.rawPhoto = strings.TrimSpace(photoText.String())
	if sawPhotoElem {
		if info.rawPhoto == "" {
			info.photo = photoNone
		} else if h := strings.ToLower(info.rawPhoto); isSHA1Hex(h) {
			info.photo = photoHash
			info.hash = h
		}
	}
	return info
}

// hasStatus reports whether the muc#user status code list contains code.
func (p presenceInfo) hasStatus(code string) bool {
	for _, c := range p.statusCodes {
		if c == code {
			return true
		}
	}
	return false
}

func isSHA1Hex(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// bareOf strips the resource from a JID string without validating it.
func bareOf(j string) string {
	if i := strings.IndexByte(j, '/'); i >= 0 {
		// A '/' inside the local part cannot occur (it is forbidden), so the first one starts the resource.
		return j[:i]
	}
	return j
}
