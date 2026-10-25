package connector

import (
	"context"
	"encoding/xml"
	"strings"
	"time"
	"unicode"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
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

	// maxInvitesPerHour caps the invites acted on per session and hour (anyone can send invites).
	maxInvitesPerHour = 10
	inviteCapWindow   = time.Hour
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

// handleInboundInvite runs on the Serve goroutine for a live invite: memory only, then hands the work
// off. Ignored: an invite from our own account, to a Cheogram number JID, to a room we are in, a repeat
// inside the interval, or any invite beyond maxInvitesPerHour acted on in the last hour.
func (x *xmppClient) handleInboundInvite(inv *mucInvite) {
	if strings.EqualFold(inv.inviter, bareJID(string(x.loginID))) || IsCheogramDMJID(inv.room) {
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
	recent := x.inviteActed[:0]
	for _, t := range x.inviteActed {
		if now.Sub(t) < inviteCapWindow {
			recent = append(recent, t)
		}
	}
	x.inviteActed = recent
	if len(x.inviteActed) >= maxInvitesPerHour {
		x.inviteMu.Unlock()
		x.log.Debug().Str("room", inv.room).Msg("Dropping MUC invite: hourly invite cap reached")
		return
	}
	x.inviteActed = append(x.inviteActed, now)
	x.inviteSeen[key] = now
	if inv.password != "" {
		if x.invitePasswords == nil {
			x.invitePasswords = map[string]string{}
		}
		x.invitePasswords[key] = inv.password
	}
	x.inviteMu.Unlock()
	go x.actOnMUCInvite(*inv)
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
	x.inviteSeen, x.invitePasswords, x.inviteActed = nil, nil, nil
	x.inviteMu.Unlock()
}

// actOnMUCInvite turns an accepted invite into a native Matrix invite, under its own root span (room
// only; the reason and password are never logged or traced). The XMPP room is never joined here: the
// portal is marked left + invite-pending (PortalMetadata.MUCLeft / MUCInvitePending) so auto-rejoin,
// view, send, the join watchdog and bookmarks leave it alone until the user accepts the Matrix invite
// (HandleMatrixMembership -> JoinMUC).
//
//   - no portal with a Matrix room: create it (ChatResync, CreatePortal) with the user as an INVITED
//     member, so bridgev2 sends only the invite and never joins the double puppet;
//   - portal exists, user not joined in Matrix: queue a resync that re-invites the user;
//   - portal exists, user joined in Matrix and the XMPP room is left: post a notice in the portal room
//     (an invite cannot be accepted when already joined) pointing at the join command.
func (x *xmppClient) actOnMUCInvite(inv mucInvite) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.invite", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.muc.room", inv.room))
	}
	log := x.logFor(ctx)
	portal, err := x.lookupMUCPortal(ctx, inv.room)
	if err != nil {
		log.Warn().Err(err).Str("room", inv.room).Msg("MUC invite: portal lookup failed; invite dropped")
		telemetry.Fail(span, err)
		return
	}
	if portal == nil || portal.MXID == "" {
		// Only a real room gets a portal: a direct invite can name any JID, and a portal created under a
		// contact's bare JID would take over that contact's DM portal key (as an invite-pending MUC).
		if (portal != nil && portal.RoomType == database.RoomTypeDM) || !x.isMUC(ctx, inv.room) {
			log.Debug().Str("room", inv.room).Msg("MUC invite: the invited JID is not a room; invite dropped")
			return
		}
		x.queueInviteResync(ctx, inv, true, inviteText(inv, "Accept the invite to join; the bridge joins the XMPP room then."))
		return
	}
	if meta, _ := portal.Metadata.(*PortalMetadata); meta == nil || !meta.MUCLeft {
		log.Debug().Str("room", inv.room).Msg("MUC invite: room is bridged and not left; nothing to do")
		return
	}
	if m, ok := x.userRoomMembership(ctx, portal.MXID); ok && m != event.MembershipJoin {
		x.queueInviteResync(ctx, inv, false, inviteText(inv, "Accept the invite to join; the bridge joins the XMPP room then."))
		return
	}
	x.notifyPortalRoom(ctx, inv.room, portal.MXID, inviteText(inv, "Send `"+x.mucCmdPrefix()+" join "+inv.room+"` here or in the management room to rejoin."))
}

// inviteText words the notice posted into the portal room. tail is the instruction that closes it.
func inviteText(inv mucInvite, tail string) string {
	who := "Someone"
	if inv.inviter != "" {
		who = "`" + inv.inviter + "`"
	}
	text := who + " invited you to `" + inv.room + "`"
	if r := sanitizeInviteReason(inv.reason); r != "" {
		text += ": " + r
	}
	text += ". " + tail
	if inv.password != "" {
		text += " (The room has a password; it was included with the invite.)"
	}
	return text
}

// inviteMemberList is the member list of an invite resync: just the user, invited (never joined).
func (x *xmppClient) inviteMemberList() *bridgev2.ChatMemberList {
	meID := networkid.UserID(x.loginID)
	return &bridgev2.ChatMemberList{
		IsFull:           false,
		TotalMemberCount: 1,
		MemberMap: bridgev2.ChatMemberMap{
			meID: {EventSender: x.makeEventSender(string(x.loginID)), Membership: event.MembershipInvite},
		},
	}
}

// markInvitePending marks the portal left + invite-pending (idempotent; reports whether it changed).
func markInvitePending(meta *PortalMetadata) bool {
	if meta.MUCLeft && meta.MUCInvitePending {
		return false
	}
	meta.MUCLeft, meta.MUCInvitePending = true, true
	return true
}

// queueInviteResync queues the ChatResync that creates (create) or re-invites into the portal of an
// invited room. The chat info carries the user as an invited member and marks the portal pending in
// ExtraUpdates, which bridgev2 runs before it creates the room; the notice is posted from the
// post-handler, once the room exists.
func (x *xmppClient) queueInviteResync(ctx context.Context, inv mucInvite, create bool, notice string) {
	room := inv.room
	// stale: the room was joined between this invite and the resync (accept, join command, bookmark).
	// GetChatInfoFunc and PostHandleFunc both run in the portal event loop, one after the other.
	stale := false
	x.queueRemoteEvent(ctx, &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			PortalKey:    networkid.PortalKey{ID: networkid.PortalID(room), Receiver: x.loginID},
			CreatePortal: create,
			PostHandleFunc: func(ctx context.Context, portal *bridgev2.Portal) {
				defer telemetry.EndRemoteEvent(ctx)
				if stale {
					return
				}
				var mxid id.RoomID
				if portal != nil {
					mxid = portal.MXID
				}
				x.notifyPortalRoom(ctx, room, mxid, notice)
			},
		},
		GetChatInfoFunc: func(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
			if portal != nil && portal.MXID != "" {
				if meta, _ := portal.Metadata.(*PortalMetadata); meta == nil || !meta.MUCLeft {
					// Re-checked here, serialized with the accept: never mark a joined room pending again.
					stale = true
					return &bridgev2.ChatInfo{}, nil
				}
			}
			info := &bridgev2.ChatInfo{}
			if create {
				var err error
				// Name, topic and type as for an imported room; no join is needed for that.
				if info, err = x.getImportedMUCChatInfo(ctx, portal); err != nil {
					return nil, err
				}
			}
			info.Members = x.inviteMemberList()
			orig := info.ExtraUpdates
			info.ExtraUpdates = func(ctx context.Context, p *bridgev2.Portal) bool {
				changed := orig != nil && orig(ctx, p)
				meta, _ := p.Metadata.(*PortalMetadata)
				if meta == nil {
					meta = &PortalMetadata{}
					p.Metadata = meta
				}
				return markInvitePending(meta) || changed
			}
			return info, nil
		},
	})
}

// userRoomMembership is the user's Matrix membership in a portal room as the bridge knows it (false
// when unknown).
func (x *xmppClient) userRoomMembership(ctx context.Context, mxid id.RoomID) (event.Membership, bool) {
	if x.inviteMembershipHook != nil {
		return x.inviteMembershipHook(mxid)
	}
	if x.conn == nil || x.conn.br == nil || x.conn.br.Matrix == nil || x.login == nil || x.login.UserMXID == "" {
		return "", false
	}
	mi, err := x.conn.br.Matrix.GetMemberInfo(ctx, mxid, x.login.UserMXID)
	if err != nil || mi == nil {
		return "", false
	}
	return mi.Membership, true
}

// notifyPortalRoom posts an m.notice from the bridge bot into the portal room (not the management room).
func (x *xmppClient) notifyPortalRoom(ctx context.Context, room string, mxid id.RoomID, text string) {
	if x.mucNoticeHook != nil {
		x.mucNoticeHook(room, text)
		return
	}
	if mxid == "" || x.conn == nil || x.conn.br == nil || x.conn.br.Bot == nil {
		return
	}
	content := format.RenderMarkdown(text, true, false)
	content.MsgType = event.MsgNotice
	if _, err := x.conn.br.Bot.SendMessage(ctx, mxid, event.EventMessage, &event.Content{Parsed: &content}, nil); err != nil {
		x.logFor(ctx).Warn().Err(err).Msg("Failed to send portal notice")
	}
}

// HandleMatrixMembership (bridgev2.MembershipHandlingNetworkAPI) makes accepting the Matrix invite of an
// invited MUC portal the explicit "join" intent: JoinMUC clears the left/pending flags, joins with the
// invite password (sendMUCJoin) and writes the bookmark. Rejecting changes nothing on XMPP. Every other
// change answers ErrMembershipNotSupported, which gives the same failed message status (no notice) and
// optional state revert as before this interface existed; the only difference is one bridgev2 error log
// line per such event (the interface cannot return "ignored"). Own joins of other portals, own profile
// changes and rejects are a no-op success. The bridge never leaves a MUC because the user left a Matrix
// room (with bridge_matrix_leave off, bridgev2 drops own leaves before calling this).
func (x *xmppClient) HandleMatrixMembership(ctx context.Context, msg *bridgev2.MatrixMembershipChange) (*bridgev2.MatrixMembershipResult, error) {
	switch msg.Type {
	case bridgev2.AcceptInvite, bridgev2.Join:
		if !x.isOwnLoginTarget(msg.Target) || msg.Portal == nil {
			return nil, bridgev2.ErrMembershipNotSupported
		}
		if meta, _ := msg.Portal.Metadata.(*PortalMetadata); meta == nil || !meta.MUCInvitePending {
			return nil, nil // an ordinary join of a bridged room: nothing to do on XMPP
		}
		room := string(msg.Portal.ID)
		if IsCheogramDMJID(room) {
			return nil, nil
		}
		if err := x.JoinMUC(ctx, room); err != nil {
			x.logFor(ctx).Warn().Err(err).Str("room", room).Msg("MUC invite accepted but the XMPP join failed")
			x.notifyPortalRoom(ctx, room, msg.Portal.MXID, "Could not join the XMPP room. Send `"+x.mucCmdPrefix()+" join "+room+"` to try again.")
			return nil, err
		}
		return nil, nil
	case bridgev2.RejectInvite:
		if x.isOwnLoginTarget(msg.Target) && msg.Portal != nil {
			x.forgetInvitePassword(string(msg.Portal.ID))
			return nil, nil
		}
		return nil, bridgev2.ErrMembershipNotSupported
	case bridgev2.ProfileChange:
		x.handleOwnProfileChange(ctx, msg)
		return nil, nil
	}
	return nil, bridgev2.ErrMembershipNotSupported
}

var _ bridgev2.MembershipHandlingNetworkAPI = (*xmppClient)(nil)

// isOwnLoginTarget reports whether a membership change targets this client's own login.
func (x *xmppClient) isOwnLoginTarget(t bridgev2.GhostOrUserLogin) bool {
	ul, ok := t.(*bridgev2.UserLogin)
	return ok && ul != nil && ul.ID == x.loginID
}

// forgetInvitePassword drops the remembered invite password of a room.
func (x *xmppClient) forgetInvitePassword(room string) {
	x.inviteMu.Lock()
	delete(x.invitePasswords, strings.ToLower(room))
	x.inviteMu.Unlock()
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
