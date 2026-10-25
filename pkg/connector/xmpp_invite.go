package connector

import (
	"context"
	"encoding/xml"
	"strings"
	"time"
	"unicode"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"mellium.im/xmpp/jid"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// MUC invites (issue #10): XEP-0045 mediated invites (<x xmlns=muc#user><invite from><reason/></invite>
// <password/></x>, sent by the room) and XEP-0249 direct invites (<x xmlns=jabber:x:conference jid
// password reason/>, sent by the inviter). Both are shown as a notice in the management room with the
// join command; the bridge never joins on its own.
const (
	conferenceNS = "jabber:x:conference" // XEP-0249

	maxInviteJIDLen      = 256
	maxInviteReasonBytes = 1024
	maxInvitePassword    = 256

	// inviteNoticeInterval is the minimum gap between two notices for the same room (clients often
	// send both invite forms), and maxInviteTracked bounds the in-memory tables against a flood of
	// invites to many different rooms.
	inviteNoticeInterval = 10 * time.Minute
	maxInviteTracked     = 256
)

// mucInvite is a validated invite. Untrusted sender data: room is a bare JID, reason is length-capped.
type mucInvite struct {
	room     string
	inviter  string // bare JID; "" when the mediated invite named none we could parse
	reason   string
	password string
	direct   bool
}

// inviteScan collects the raw invite elements of one stanza from the token stream (memory only).
type inviteScan struct {
	// direct (XEP-0249) <x> as a direct child.
	haveDirect                               bool
	directRoom, directReason, directPassword string
	// mediated (XEP-0045) <x><invite/><password/></x>.
	xDepth, inviteDepth             int
	haveInvite                      bool
	invFrom, invReason, invPassword string
	target                          *string
	targetDepth                     int
	buf                             []byte
}

func capBytes(s string, n int) string {
	if len(s) > n {
		s = s[:n]
	}
	return strings.ToValidUTF8(s, "")
}

// start observes a start element at depth (direct children of the message are at childDepth).
func (s *inviteScan) start(se xml.StartElement, depth, childDepth int) {
	switch {
	case depth == childDepth && se.Name.Local == "x" && se.Name.Space == conferenceNS:
		if !s.haveDirect {
			s.haveDirect = true
			s.directRoom = capBytes(attrValue(se.Attr, "jid"), maxInviteJIDLen+1)
			s.directReason = capBytes(attrValue(se.Attr, "reason"), maxInviteReasonBytes)
			if pw := attrValue(se.Attr, "password"); len(pw) <= maxInvitePassword {
				s.directPassword = pw
			}
		}
	case depth == childDepth && se.Name.Local == "x" && se.Name.Space == mucUserNS:
		s.xDepth = depth
	case s.xDepth != 0 && depth == s.xDepth+1 && se.Name.Space == mucUserNS:
		switch se.Name.Local {
		case "invite":
			if !s.haveInvite {
				s.haveInvite = true
				s.inviteDepth = depth
				s.invFrom = capBytes(attrValue(se.Attr, "from"), maxInviteJIDLen+1)
			}
		case "password":
			if s.invPassword == "" && s.target == nil {
				s.target, s.targetDepth, s.buf = &s.invPassword, depth, s.buf[:0]
			}
		}
	case s.inviteDepth != 0 && depth == s.inviteDepth+1 && se.Name.Local == "reason" && se.Name.Space == mucUserNS:
		if s.invReason == "" && s.target == nil {
			s.target, s.targetDepth, s.buf = &s.invReason, depth, s.buf[:0]
		}
	}
}

// chars collects the text of the <reason>/<password> being read.
func (s *inviteScan) chars(cd xml.CharData) {
	if s.target != nil && len(s.buf) < maxInviteReasonBytes+4 {
		s.buf = append(s.buf, cd...)
	}
}

// end observes the end of the element at depth.
func (s *inviteScan) end(depth int) {
	if s.target != nil && depth == s.targetDepth {
		if s.target == &s.invPassword {
			if len(s.buf) <= maxInvitePassword {
				*s.target = string(s.buf)
			}
		} else {
			*s.target = capBytes(string(s.buf), maxInviteReasonBytes)
		}
		s.target, s.buf = nil, s.buf[:0]
	}
	if depth == s.inviteDepth {
		s.inviteDepth = 0
	}
	if depth == s.xDepth {
		s.xDepth = 0
	}
}

// bareValidJID returns the lower-case-preserved bare form of s when it is a valid bare JID with a
// local part and no characters that could break out of the notice's formatting.
func bareValidJID(s string) (string, bool) {
	if s == "" || len(s) > maxInviteJIDLen || strings.ContainsAny(s, "`*_[]()<>\\ \t\r\n") {
		return "", false
	}
	j, err := jid.Parse(s)
	if err != nil || j.Localpart() == "" || j.Resourcepart() != "" {
		return "", false
	}
	return j.String(), true
}

// inviteFrom resolves the invite carried by a stanza sent from `from` (nil when none or invalid). A
// mediated invite must come from the bare room JID; a direct invite names the room in its jid attribute
// and comes from the inviter.
func (m *messageIDs) inviteFrom(from string) *mucInvite {
	s := m.invite
	if s == nil {
		return nil
	}
	if s.haveInvite {
		if from != bareJID(from) {
			return nil
		}
		room, ok := bareValidJID(from)
		if !ok {
			return nil
		}
		inviter, _ := bareValidJID(bareJID(s.invFrom))
		return &mucInvite{room: room, inviter: inviter, reason: s.invReason, password: s.invPassword}
	}
	if s.haveDirect {
		room, ok := bareValidJID(s.directRoom)
		if !ok {
			return nil
		}
		inviter, _ := bareValidJID(bareJID(from))
		return &mucInvite{room: room, inviter: inviter, reason: s.directReason, password: s.directPassword, direct: true}
	}
	return nil
}

// handleInboundInvite runs on the Serve goroutine for a live invite: memory only, then hands the notice
// off. Ignored: an invite from our own account, to a room we are in, or a repeat inside the interval.
func (x *xmppClient) handleInboundInvite(inv *mucInvite) {
	if strings.EqualFold(inv.inviter, bareJID(string(x.loginID))) {
		return
	}
	if x.isJoinedRoomBare(inv.room) {
		x.log.Debug().Str("room", inv.room).Msg("Ignoring MUC invite to a room we are already in")
		return
	}
	key := strings.ToLower(inv.room)
	now := time.Now()
	x.inviteMu.Lock()
	if x.inviteSeen == nil {
		x.inviteSeen = map[string]time.Time{}
	}
	if len(x.inviteSeen) >= maxInviteTracked {
		for k, t := range x.inviteSeen {
			if now.Sub(t) >= inviteNoticeInterval {
				delete(x.inviteSeen, k)
				delete(x.invitePasswords, k)
			}
		}
	}
	last, seen := x.inviteSeen[key]
	if (seen && now.Sub(last) < inviteNoticeInterval) || (!seen && len(x.inviteSeen) >= maxInviteTracked) {
		x.inviteMu.Unlock()
		x.log.Debug().Str("room", inv.room).Msg("Ignoring repeated MUC invite")
		return
	}
	x.inviteSeen[key] = now
	if inv.password != "" {
		if x.invitePasswords == nil {
			x.invitePasswords = map[string]string{}
		}
		x.invitePasswords[key] = inv.password
	}
	x.inviteMu.Unlock()
	go x.notifyMUCInvite(*inv)
}

// invitePassword is the room password that came with an invite this session ("" when none). Never log it.
func (x *xmppClient) invitePassword(room string) string {
	x.inviteMu.Lock()
	defer x.inviteMu.Unlock()
	return x.invitePasswords[strings.ToLower(room)]
}

// resetInvites forgets the invite state of the previous session.
func (x *xmppClient) resetInvites() {
	x.inviteMu.Lock()
	x.inviteSeen, x.invitePasswords = nil, nil
	x.inviteMu.Unlock()
}

// notifyMUCInvite posts the invite notice to the management room, under its own root span (room only;
// the reason and password are never logged or traced).
func (x *xmppClient) notifyMUCInvite(inv mucInvite) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.invite", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.muc.room", inv.room))
	}
	who := "Someone"
	if inv.inviter != "" {
		who = "`" + inv.inviter + "`"
	}
	text := who + " invited you to `" + inv.room + "`"
	if r := sanitizeInviteReason(inv.reason); r != "" {
		text += ": " + r
	}
	text += ". Use `" + x.mucCmdPrefix() + " join " + inv.room + "` to join."
	if inv.password != "" {
		text += " (The room has a password; it was included with the invite.)"
	}
	x.notifyManagement(ctx, text)
}

// sanitizeInviteReason drops control characters, flattens whitespace, caps the length and escapes
// Markdown so that the reason renders as plain text.
func sanitizeInviteReason(s string) string {
	var b strings.Builder
	n := 0
	for _, r := range s {
		if n >= 300 {
			b.WriteString("...")
			break
		}
		switch {
		case unicode.IsSpace(r):
			r = ' '
		case unicode.IsControl(r) || unicode.Is(unicode.Cf, r):
			continue
		}
		if strings.ContainsRune("\\`*_{}[]()#+-.!|<>~&", r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
		n++
	}
	return strings.TrimSpace(b.String())
}
