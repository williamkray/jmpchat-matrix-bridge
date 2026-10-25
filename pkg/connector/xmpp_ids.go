package connector

import (
	"encoding/xml"
	"strings"
)

// Stable-ID elements (XEP-0359 unique and stable stanza IDs, XEP-0421 occupant IDs). They are
// captured so that later features (retractions, moderation) can map an XMPP reference to the
// bridgev2 message row; see MessageMetadata.
const (
	sidNS        = "urn:xmpp:sid:0"
	occupantIDNS = "urn:xmpp:occupant-id:0"

	// maxStanzaIDRefs bounds how many <stanza-id> elements one stanza can make us remember, and
	// maxStableIDLen bounds a single id; both only limit what a hostile sender can make us hold.
	maxStanzaIDRefs = 8
	maxStableIDLen  = 1024
)

// stanzaIDRef is one <stanza-id by=... id=.../> child.
type stanzaIDRef struct {
	By string
	ID string
}

// messageIDs holds the XEP-0359/0421 identifiers found as DIRECT children of a <message>.
// Nothing here is trusted until stanzaIDBy picks an entry whose `by` is an entity we trust.
type messageIDs struct {
	StanzaIDs  []stanzaIDRef
	OriginID   string
	OccupantID string // XEP-0421 occupant-id; only meaningful for groupchat

	// ReplyFallback is the XEP-0428 fallback range for a XEP-0461 reply (offsets relative to the
	// trimmed body); filled by parseMessageBodyOOBIDs only.
	ReplyFallback replyFallback

	// Retract is the XEP-0424/0425 retraction carried by the stanza (nil when none), and Tombstone is
	// set for an archived original that was retracted (<retracted>). Both are filled by finish().
	Retract   *retractInfo
	Tombstone bool
	scan      *retractScan

	// Mentions are the XEP-0372 mention references (direct children of <message>), offsets in code
	// points of the body as sent; parseMessageBodyOOBIDs shifts them to the trimmed body. At most
	// maxMentionRefs, malformed ones are dropped at parse time.
	Mentions []mentionRef

	// Extras holds the XEP-0308/0085/0333/0184/subject/E2EE signals (see messageExtras); filled while
	// tracking, the subject is complete once its end element was tracked.
	Extras       messageExtras
	invite       *inviteScan // XEP-0045/0249 invite elements; resolved by inviteFrom
	subjectDepth int         // depth of the <subject> being read; 0 when not inside one
	subjectBuf   []byte
}

// track feeds a start element of any depth to the identifier and retraction scanners; depth is the
// element's depth and childDepth the depth of direct children of the <message>.
func (m *messageIDs) track(se xml.StartElement, depth, childDepth int) {
	if depth == childDepth {
		m.observe(se, depth)
	}
	if m.invite == nil {
		m.invite = &inviteScan{}
	}
	m.invite.start(se, depth, childDepth)
	if m.scan == nil {
		m.scan = &retractScan{}
	}
	m.scan.start(se, depth, childDepth)
}

// trackChars feeds character data to the retraction scanner (reason text).
func (m *messageIDs) trackChars(cd xml.CharData) {
	// A rune is at most 4 bytes: this only bounds the buffer, capSubject cuts to the rune cap.
	if limit := 4*maxSubjectRunes + 4; m.subjectDepth > 0 && len(m.subjectBuf) < limit {
		if room := limit - len(m.subjectBuf); len(cd) > room {
			cd = cd[:room] // capSubject trims a cut rune at the end
		}
		m.subjectBuf = append(m.subjectBuf, cd...)
	}
	if m.invite != nil {
		m.invite.chars(cd)
	}
	if m.scan != nil {
		m.scan.chars(cd)
	}
}

// trackEnd feeds an end element (depth of the element that closes) to the retraction scanner.
func (m *messageIDs) trackEnd(depth int) {
	if m.subjectDepth > 0 && depth == m.subjectDepth {
		s := capSubject(strings.ToValidUTF8(string(m.subjectBuf), "")) // drop a rune cut by the byte budget
		m.Extras.Subject = &s
		m.subjectDepth = 0
		m.subjectBuf = m.subjectBuf[:0]
	}
	if m.invite != nil {
		m.invite.end(depth)
	}
	if m.scan != nil {
		m.scan.end(depth)
	}
}

// finish resolves the retraction state once the whole message was read. Callers must drop the body
// of the message when Retract is set: it is the XEP-0424 fallback text and must never be bridged.
func (m *messageIDs) finish() {
	if m.scan == nil {
		return
	}
	m.Retract = m.scan.result()
	m.Tombstone = m.scan.tombstone
	m.scan = nil
}

// observe inspects a start element that is a direct child of the message (the caller checks the
// depth). Matching is on the resolved namespace, not on a literal xmlns attribute, so children that
// inherit their namespace (forwarded carbons/MAM) are still found.
func (m *messageIDs) observe(se xml.StartElement, depth int) {
	if m.Extras.observe(se) {
		m.subjectDepth = depth
		m.subjectBuf = m.subjectBuf[:0]
	}
	switch {
	case se.Name.Space == referenceNS && se.Name.Local == "reference":
		if r, ok := parseMentionRef(se.Attr); ok && len(m.Mentions) < maxMentionRefs {
			m.Mentions = append(m.Mentions, r)
		}
	case se.Name.Space == sidNS && se.Name.Local == "stanza-id":
		by, id := attrValue(se.Attr, "by"), attrValue(se.Attr, "id")
		if by != "" && id != "" && len(id) <= maxStableIDLen && len(m.StanzaIDs) < maxStanzaIDRefs {
			m.StanzaIDs = append(m.StanzaIDs, stanzaIDRef{By: by, ID: id})
		}
	case se.Name.Space == sidNS && se.Name.Local == "origin-id":
		if id := attrValue(se.Attr, "id"); m.OriginID == "" && len(id) <= maxStableIDLen {
			m.OriginID = id
		}
	case se.Name.Space == occupantIDNS && se.Name.Local == "occupant-id":
		if id := attrValue(se.Attr, "id"); m.OccupantID == "" && len(id) <= maxStableIDLen {
			m.OccupantID = id
		}
	}
}

// stanzaIDBy returns the stanza-id assigned by the given entity (bare JID, compared
// case-insensitively), or "". A stanza-id from any other `by` must not be believed: that is the room
// for groupchat and our own account for 1:1 chats.
func (m messageIDs) stanzaIDBy(by string) string {
	by = bareJID(by)
	if by == "" {
		return ""
	}
	for _, s := range m.StanzaIDs {
		if strings.EqualFold(s.By, by) {
			return s.ID
		}
	}
	return ""
}
