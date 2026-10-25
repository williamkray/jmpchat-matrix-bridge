package connector

import (
	"encoding/xml"
	"strings"
	"time"
	"unicode/utf8"
)

// Namespaces of the inbound signals recognized by messageExtras. Phase A only parses them; the consumers
// (corrections, chat states, markers, receipts, subject, encryption notices) come in later phases.
const (
	correctNS     = "urn:xmpp:message-correct:0"            // XEP-0308
	chatStatesNS  = "http://jabber.org/protocol/chatstates" // XEP-0085
	markersNS     = "urn:xmpp:chat-markers:0"               // XEP-0333
	receiptsNS    = "urn:xmpp:receipts"                     // XEP-0184
	omemoLegacyNS = "eu.siacs.conversations.axolotl"        // OMEMO 0.3 <encrypted>
	omemo2NS      = "urn:xmpp:omemo:2"                      // OMEMO 2 <encrypted>
	pgpLegacyNS   = "jabber:x:encrypted"                    // XEP-0027 <x>
	openpgpNS     = "urn:xmpp:openpgp:0"                    // XEP-0373 <openpgp>
	emeNS         = "urn:xmpp:eme:0"                        // XEP-0380 <encryption namespace=...>

	// maxSubjectRunes bounds the subject text we keep from a hostile sender.
	maxSubjectRunes = 4096

	emeKindPrefix = "eme:"
)

// maxDelayRefs bounds how many direct-child <delay> elements one stanza can make us remember.
const maxDelayRefs = 4

// delayRef is one parsed XEP-0203 <delay from stamp/> child (untrusted; see trustedDelay).
type delayRef struct {
	From  string
	Stamp time.Time
}

// parseDelayStamp parses a XEP-0082 date-time (CCYY-MM-DDThh:mm:ss[.sss]Z).
func parseDelayStamp(s string) (time.Time, bool) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// messageExtras holds the XEP-0308/0085/0333/0184/0045-subject/E2EE signals found as DIRECT children of a
// <message>. Untrusted sender data; consumers validate.
type messageExtras struct {
	Replace         string     // <replace xmlns=correctNS id>; "" none
	ChatState       string     // "active","composing","paused","inactive","gone"; "" none
	Markable        bool       // <markable xmlns=markersNS/>
	Displayed       string     // <displayed xmlns=markersNS id>
	ReceiptRequest  bool       // <request xmlns=receiptsNS/>
	ReceiptReceived string     // <received xmlns=receiptsNS id>
	Subject         *string    // <subject> (stanza namespace); non-nil even when empty (= subject cleared)
	Delayed         bool       // direct-child <delay xmlns=delayNS>: a replayed (history/offline) stanza, not a live one
	Delays          []delayRef // direct-child <delay> elements with a parsable stamp (max maxDelayRefs)
	Encryption      string     // "omemo","omemo2","openpgp","pgp","eme:<ns>"; "" none
}

// isStanzaNS reports whether a resolved namespace is one a <subject> child of a (possibly forwarded)
// message can carry: none, jabber:client, or the namespace inherited from a forwarded/carbon wrapper.
func isStanzaNS(space string) bool {
	return space == "" || space == "jabber:client" || space == forwardNS || space == carbonsNS || space == mamNS
}

// setID stores id into dst when dst is still empty and id is within the length cap.
func setID(dst *string, id string) {
	if *dst == "" && len(id) <= maxStableIDLen {
		*dst = id
	}
}

// observe inspects a start element that is a direct child of the message (the caller checks the depth).
// Matching is on the resolved namespace, never a literal xmlns attribute. It reports whether the element
// is the first <subject>, whose text the caller must collect.
func (e *messageExtras) observe(se xml.StartElement) (subject bool) {
	switch se.Name.Space {
	case correctNS:
		if se.Name.Local == "replace" {
			setID(&e.Replace, attrValue(se.Attr, "id"))
		}
	case chatStatesNS:
		switch se.Name.Local {
		case "active", "composing", "paused", "inactive", "gone":
			if e.ChatState == "" {
				e.ChatState = se.Name.Local
			}
		}
	case markersNS:
		switch se.Name.Local {
		case "markable":
			e.Markable = true
		case "displayed":
			setID(&e.Displayed, attrValue(se.Attr, "id"))
		}
	case receiptsNS:
		switch se.Name.Local {
		case "request":
			e.ReceiptRequest = true
		case "received":
			setID(&e.ReceiptReceived, attrValue(se.Attr, "id"))
		}
	case delayNS:
		if se.Name.Local == "delay" {
			e.Delayed = true
			from := attrValue(se.Attr, "from")
			if stamp, ok := parseDelayStamp(attrValue(se.Attr, "stamp")); ok && len(from) <= maxStableIDLen && len(e.Delays) < maxDelayRefs {
				e.Delays = append(e.Delays, delayRef{From: from, Stamp: stamp})
			}
		}
	case omemoLegacyNS:
		if se.Name.Local == "encrypted" {
			e.setEncryption("omemo")
		}
	case omemo2NS:
		if se.Name.Local == "encrypted" {
			e.setEncryption("omemo2")
		}
	case pgpLegacyNS:
		if se.Name.Local == "x" {
			e.setEncryption("pgp")
		}
	case openpgpNS:
		if se.Name.Local == "openpgp" {
			e.setEncryption("openpgp")
		}
	case emeNS:
		if se.Name.Local == "encryption" {
			if ns := attrValue(se.Attr, "namespace"); ns != "" && len(ns) <= maxStableIDLen {
				e.setEncryption(emeKindPrefix + ns)
			}
		}
	}
	return se.Name.Local == "subject" && isStanzaNS(se.Name.Space) && e.Subject == nil
}

// setEncryption records an encryption marker: the first concrete element wins, and an EME hint only
// counts while no concrete element was seen (a concrete element replaces an EME hint).
func (e *messageExtras) setEncryption(kind string) {
	isEME := strings.HasPrefix(kind, emeKindPrefix)
	switch {
	case e.Encryption == "":
		e.Encryption = kind
	case !isEME && strings.HasPrefix(e.Encryption, emeKindPrefix):
		e.Encryption = kind
	}
}

// capSubject trims the ends of a collected subject and caps it at maxSubjectRunes runes; internal
// whitespace is kept.
func capSubject(s string) string {
	s = strings.TrimSpace(s)
	if utf8.RuneCountInString(s) > maxSubjectRunes {
		s = strings.TrimSpace(string([]rune(s)[:maxSubjectRunes]))
	}
	return s
}
