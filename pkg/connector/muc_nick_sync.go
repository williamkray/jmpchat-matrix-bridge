package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Nick sync (network.muc_nick_sync): the user's per-room Matrix display name and their MUC nick follow
// each other. Matrix -> XMPP: an own profile change in a bridged MUC room (debounced) becomes a nick
// change, unless the new name is the user's global display name (which is never sent). XMPP -> Matrix:
// own nick changes (confirmed rename, foreign rename by another client, 409 alternative, server-assigned
// nick) set the per-room display name through the double puppet. Matrix display names are never logged.

// mucNickSync reports network.muc_nick_sync (default true; a nil config means the default).
func (c *JabberConnector) mucNickSync() bool {
	if c == nil || c.networkConfig == nil || c.networkConfig.MUCNickSync == nil {
		return true
	}
	return *c.networkConfig.MUCNickSync
}

func (x *xmppClient) nickSyncOn() bool { return x.conn.mucNickSync() }

var nickSyncDebounce = 3 * time.Second // quiet period after the last room display name change

const (
	globalProfileTTL = 30 * time.Second
	nickSyncMapCap   = 4096
)

// nickSyncReq is one debounced Matrix -> XMPP nick request.
type nickSyncReq struct {
	timer       *time.Timer
	display     string // requested room display name (never logged)
	prevDisplay string // PrevContent displayname (revert target; kept from the first change of a burst)
	portalMXID  id.RoomID
}

// nickSyncState holds the debounced Matrix -> XMPP nick requests (one per portal).
type nickSyncState struct {
	mu      sync.Mutex
	pending map[networkid.PortalID]*nickSyncReq
}

func (s *nickSyncState) reset() {
	s.mu.Lock()
	for _, r := range s.pending {
		r.timer.Stop()
	}
	s.pending = nil
	s.mu.Unlock()
}

// schedule (re)arms the single pending timer of the portal; when it fires without a newer request,
// fire is called from the timer goroutine. The revert target of the first request of a burst is kept.
func (s *nickSyncState) schedule(portal networkid.PortalID, req *nickSyncReq, d time.Duration, fire func(*nickSyncReq)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.pending[portal]; ok {
		old.timer.Stop()
		req.prevDisplay = old.prevDisplay
		delete(s.pending, portal)
	}
	if s.pending == nil || len(s.pending) >= nickSyncMapCap {
		for _, r := range s.pending {
			r.timer.Stop()
		}
		s.pending = map[networkid.PortalID]*nickSyncReq{}
	}
	req.timer = time.AfterFunc(d, func() {
		s.mu.Lock()
		if s.pending[portal] != req {
			s.mu.Unlock()
			return
		}
		delete(s.pending, portal)
		s.mu.Unlock()
		fire(req)
	})
	s.pending[portal] = req
}

// nickSyncPortalOK reports whether a portal takes part in the sync: a bridged MUC of this login that is
// neither left nor an unanswered invite (DMs, Cheogram and portals without a Matrix room never).
func (x *xmppClient) nickSyncPortalOK(p *database.Portal) bool {
	if p == nil || p.MXID == "" || p.Receiver != x.loginID {
		return false
	}
	if p.RoomType == database.RoomTypeDM || IsCheogramDMJID(string(p.ID)) {
		return false
	}
	if p.RoomType == "" && !isMUCJID(string(p.ID)) {
		return false
	}
	if meta, _ := p.Metadata.(*PortalMetadata); meta != nil && (meta.MUCLeft || meta.MUCInvitePending) {
		return false
	}
	return true
}

// handleOwnProfileChange is the ProfileChange branch of HandleMatrixMembership (portal event loop; memory
// only, no HTTP): a changed per-room display name of the user is scheduled as a nick change.
func (x *xmppClient) handleOwnProfileChange(ctx context.Context, msg *bridgev2.MatrixMembershipChange) {
	if !x.nickSyncOn() || msg == nil {
		return
	}
	if msg.Portal == nil || msg.Portal.Portal == nil || msg.OrigSender != nil || !x.isOwnLoginTarget(msg.Target) {
		return
	}
	if x.login == nil || msg.Event == nil || msg.Event.Sender != x.login.UserMXID || msg.Event.GetStateKey() != string(x.login.UserMXID) {
		return
	}
	if _, dp := msg.Event.Content.Raw[appservice.DoublePuppetKey]; dp {
		return // our own double-puppet write (bridgev2 drops these too)
	}
	var newName, oldName string
	if msg.Content != nil {
		newName = msg.Content.Displayname
	}
	if msg.PrevContent != nil {
		oldName = msg.PrevContent.Displayname
	}
	if newName == oldName || strings.TrimSpace(newName) == "" {
		return // avatar-only change, or a cleared name
	}
	if !x.nickSyncPortalOK(msg.Portal.Portal) {
		return
	}
	portalID := msg.Portal.ID
	req := &nickSyncReq{display: newName, prevDisplay: oldName, portalMXID: msg.Portal.MXID}
	x.logFor(ctx).Debug().Str("room", string(portalID)).Int("display_len", len(newName)).Msg("Room display name changed, scheduling MUC nick sync")
	x.nickSync.schedule(portalID, req, nickSyncDebounce, func(r *nickSyncReq) { x.runNickSyncOut(portalID, r) })
}

// runNickSyncOut applies a debounced room display name as a MUC nick (timer goroutine).
func (x *xmppClient) runNickSyncOut(portalID networkid.PortalID, r *nickSyncReq) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.nick_sync_out", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	log := x.logFor(ctx)
	room := string(portalID)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", room))
	}
	if !x.nickSyncOn() {
		return
	}
	portal, err := x.lookupMUCPortal(ctx, room)
	if err != nil || !x.nickSyncPortalOK(portal) {
		return
	}
	global, err := x.globalDisplayName(ctx)
	if err != nil {
		log.Warn().Err(err).Str("room", room).Msg("Cannot read the global display name, room nick not synced")
		return
	}
	if r.display == global {
		log.Debug().Str("room", room).Msg("Room display name equals the global one, ignored")
		return
	}
	nick, reason := sanitizeMUCNick(room, r.display)
	occ := x.getMUCOurOccupantJID(room)
	if reason != "" {
		cur := resourceOf(occ)
		if cur == "" {
			_, cur = x.mucNickChoice(ctx, room)
		}
		log.Info().Str("room", room).Int("display_len", len(r.display)).Str("reason", reason).Msg("Room display name unusable as MUC nick")
		x.notifyPortalRoom(ctx, room, r.portalMXID, mucNickSyncInvalidNotice(cur, reason))
		return
	}
	if occ == "" {
		// Not in the room (not connected, not joined, join in flight): used at the next join.
		if err := x.saveExplicitMUCNick(ctx, room, nick); err != nil {
			telemetry.Fail(span, err)
			log.Warn().Err(err).Str("room", room).Msg("Failed to store the MUC nick from the room display name")
		}
		return
	}
	cur := resourceOf(occ)
	if cur == nick {
		meta, _ := portal.Metadata.(*PortalMetadata)
		if meta == nil || meta.MUCPreferredNick != nick {
			if err := x.saveExplicitMUCNick(ctx, room, nick); err != nil {
				log.Warn().Err(err).Str("room", room).Msg("Failed to store the MUC nick from the room display name")
			}
		}
		return
	}
	ch := &mucNickChange{want: nick, old: cur, origin: nickBySync, persist: true, prevDisplay: r.prevDisplay, display: r.display, portalMXID: r.portalMXID}
	if err := x.startMUCNickChange(ctx, room, ch); err != nil {
		telemetry.Fail(span, err)
		reverted := x.revertOwnRoomDisplayName(ctx, ch)
		x.notifyPortalRoom(ctx, room, r.portalMXID, mucNickSyncFailedNotice(nick, cur, "not connected", reverted))
	}
}

// globalDisplayName returns the user's global Matrix display name (cached globalProfileTTL).
func (x *xmppClient) globalDisplayName(ctx context.Context) (string, error) {
	x.globalNameMu.Lock()
	if !x.globalNameAt.IsZero() && time.Since(x.globalNameAt) < globalProfileTTL {
		name := x.globalName
		x.globalNameMu.Unlock()
		return name, nil
	}
	x.globalNameMu.Unlock()
	var name string
	var err error
	switch {
	case x.globalProfileHook != nil:
		name, err = x.globalProfileHook(ctx)
	case x.conn == nil || x.conn.br == nil || x.login == nil:
		err = fmt.Errorf("no bridge")
	default:
		mc, ok := x.conn.br.Matrix.(*matrix.Connector)
		if !ok || mc == nil || mc.Bot == nil {
			err = fmt.Errorf("unsupported Matrix connector")
			break
		}
		prof, perr := mc.Bot.GetProfile(ctx, x.login.UserMXID)
		if perr != nil {
			err = perr
			break
		}
		name = prof.DisplayName
	}
	if err != nil {
		return "", err
	}
	x.globalNameMu.Lock()
	x.globalName, x.globalNameAt = name, time.Now()
	x.globalNameMu.Unlock()
	return name, nil
}

// syncOwnNickToMatrixAsync is the goroutine entry used from the Serve goroutine.
func (x *xmppClient) syncOwnNickToMatrixAsync(roomBare, nick string) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.nick_to_matrix", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomBare))
	}
	x.syncOwnNickToMatrix(ctx, roomBare, nick)
}

// onForeignOwnNickChange handles a rename of our occupant that we did not ask for (another client sharing
// the nick): it becomes the room's explicit nick, so reconnects do not rename back, and is synced to Matrix.
func (x *xmppClient) onForeignOwnNickChange(roomBare, nick string) {
	if !x.nickSyncOn() {
		return
	}
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.nick_foreign", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomBare))
	}
	if err := x.saveExplicitMUCNick(ctx, roomBare, nick); err != nil {
		telemetry.Fail(span, err)
		x.logFor(ctx).Warn().Err(err).Str("room", roomBare).Msg("Failed to store the foreign MUC nick change")
	}
	x.syncOwnNickToMatrix(ctx, roomBare, nick)
}

// ownDoublePuppet returns the user's double puppet intent (nil, false when there is none).
func (x *xmppClient) ownDoublePuppet(ctx context.Context) (bridgev2.MatrixAPI, bool) {
	if x.doublePuppetHook != nil {
		return nil, x.doublePuppetHook()
	}
	if x.login == nil || x.login.User == nil {
		return nil, false
	}
	dp := x.login.User.DoublePuppet(ctx)
	return dp, dp != nil
}

// readOwnMember returns the full raw member event content of the user in the room.
func (x *xmppClient) readOwnMember(ctx context.Context, room id.RoomID) (raw map[string]any, membership, display string, err error) {
	if x.memberStateHook != nil {
		raw, err = x.memberStateHook(room)
	} else {
		raw, err = x.readOwnMemberReal(ctx, room)
	}
	if err != nil {
		return nil, "", "", err
	}
	membership, _ = raw["membership"].(string)
	display, _ = raw["displayname"].(string)
	return raw, membership, display, nil
}

func (x *xmppClient) readOwnMemberReal(ctx context.Context, room id.RoomID) (map[string]any, error) {
	if x.conn == nil || x.conn.br == nil || x.login == nil {
		return nil, fmt.Errorf("no bridge")
	}
	if cs, ok := x.conn.br.Matrix.(bridgev2.MatrixConnectorWithArbitraryRoomState); ok {
		evt, err := cs.GetStateEvent(ctx, room, event.StateMember, string(x.login.UserMXID))
		if err != nil {
			return nil, err
		}
		if evt != nil && evt.Content.Raw != nil {
			return evt.Content.Raw, nil
		}
		if evt != nil && evt.Content.Parsed != nil {
			return marshalToMap(evt.Content.Parsed)
		}
		return nil, fmt.Errorf("member event has no content")
	}
	mi, err := x.conn.br.Matrix.GetMemberInfo(ctx, room, x.login.UserMXID)
	if err != nil {
		return nil, err
	}
	if mi == nil {
		return nil, fmt.Errorf("no member info")
	}
	return marshalToMap(mi)
}

func marshalToMap(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err = json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// writeOwnDisplayname sets the displayname of the user's member event through the double puppet, keeping
// every other key (avatar_url, unknown keys). The double puppet marker added by the intent has no ts, so
// bridgev2 drops the resulting event (no loop).
func (x *xmppClient) writeOwnDisplayname(ctx context.Context, dp bridgev2.MatrixAPI, room id.RoomID, raw map[string]any, name string) error {
	newRaw := make(map[string]any, len(raw))
	for k, v := range raw {
		newRaw[k] = v
	}
	newRaw["displayname"] = name
	for _, k := range []string{"reason", appservice.DoublePuppetKey, appservice.DoublePuppetTSKey, "com.beeper.member_rollback"} {
		delete(newRaw, k)
	}
	if x.memberSetHook != nil {
		return x.memberSetHook(room, newRaw)
	}
	if dp == nil || x.login == nil {
		return fmt.Errorf("no double puppet")
	}
	_, err := dp.SendState(ctx, room, event.StateMember, string(x.login.UserMXID), &event.Content{Raw: newRaw}, time.Now())
	return err
}

// syncOwnNickToMatrix sets the user's per-room Matrix display name to nick (double puppet only; never on
// the Serve goroutine).
func (x *xmppClient) syncOwnNickToMatrix(ctx context.Context, roomBare, nick string) {
	if !x.nickSyncOn() {
		return
	}
	log := x.logFor(ctx)
	portal, err := x.lookupMUCPortal(ctx, roomBare)
	if err != nil || !x.nickSyncPortalOK(portal) {
		return
	}
	dp, ok := x.ownDoublePuppet(ctx)
	if !ok {
		log.Debug().Str("room", roomBare).Msg("No double puppet, MUC nick not synced to the room display name")
		return
	}
	raw, membership, display, err := x.readOwnMember(ctx, portal.MXID)
	if err != nil {
		log.Debug().Err(err).Str("room", roomBare).Msg("Cannot read the member event, MUC nick not synced")
		return
	}
	if membership != string(event.MembershipJoin) || display == nick {
		return // setting a display name of a non-member would join the user
	}
	if err := x.writeOwnDisplayname(ctx, dp, portal.MXID, raw, nick); err != nil {
		log.Warn().Err(err).Str("room", roomBare).Msg("Failed to set the room display name from the MUC nick")
		return
	}
	log.Info().Str("room", roomBare).Int("nick_len", len(nick)).Msg("Room display name set from the MUC nick")
}

// revertOwnRoomDisplayName puts the per-room display name back to its previous value after a refused
// sync-origin nick change, only while it still is the requested one. It reports whether it wrote.
func (x *xmppClient) revertOwnRoomDisplayName(ctx context.Context, ch *mucNickChange) bool {
	if ch.origin != nickBySync || ch.portalMXID == "" || ch.prevDisplay == "" {
		return false
	}
	dp, ok := x.ownDoublePuppet(ctx)
	if !ok {
		return false
	}
	raw, membership, display, err := x.readOwnMember(ctx, ch.portalMXID)
	if err != nil || membership != string(event.MembershipJoin) || display != ch.display {
		return false
	}
	if err := x.writeOwnDisplayname(ctx, dp, ch.portalMXID, raw, ch.prevDisplay); err != nil {
		x.logFor(ctx).Warn().Err(err).Msg("Failed to revert the room display name")
		return false
	}
	return true
}
