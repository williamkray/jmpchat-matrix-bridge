package connector

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// mucMaxNickAlternatives is how many alternative nicks (base_1 .. base_N) are tried after the
// preferred one got a 409 nick conflict.
const mucMaxNickAlternatives = 3

// mucJoinTrack follows one join attempt chain for a room (guarded by xmppClient.mucPendingJoinMu).
type mucJoinTrack struct {
	base       string      // preferred nick for this room when the chain started
	tried      []string    // nicks that were answered with 409 in this chain
	conflicted bool        // at least one 409 happened (so a confirmed join is announced)
	hist       *mucHistory // history request of the first join, repeated by the 409 retries
	// writeBookmark: the join was asked for by the explicit join command, so once it is confirmed the
	// room's XEP-0402 bookmark is created/enabled (xep0402_write.go). Auto-rejoin, imports, sending and
	// viewing never write.
	writeBookmark bool
}

// chooseMUCNick picks the base (preferred) nick and the nick to actually join with. Precedence of the
// base: explicit (the per-room muc-nick) > bookmarkNick (the room's XEP-0402 bookmark) > def (the
// muc-nick-default) > login localpart. 409 alternatives derive from the base (bookmarkNick_1, ...). An automatically chosen alternative (autoNick) is only
// reused while it was derived from the same base: changing muc-nick or muc-nick-default makes the
// bridge try the new preferred nick again.
func chooseMUCNick(explicit, bookmarkNick, def, loginID, autoNick, autoBase string) (base, nick string) {
	switch {
	case explicit != "":
		base = explicit
	case bookmarkNick != "":
		base = bookmarkNick
	case def != "":
		base = def
	default:
		base = loginID
		if at := strings.IndexByte(base, '@'); at > 0 {
			base = base[:at]
		}
		if base == "" {
			base = "bridge"
		}
	}
	if autoNick != "" && autoBase == base {
		return base, autoNick
	}
	return base, base
}

// setExplicitMUCNick records a user-chosen nick and forgets any automatic alternative.
func setExplicitMUCNick(meta *PortalMetadata, nick string) {
	meta.MUCPreferredNick = nick
	meta.MUCAutoNick = ""
	meta.MUCAutoNickBase = ""
}

// mucNickChoice returns the preferred and the effective join nick for a room.
func (x *xmppClient) mucNickChoice(ctx context.Context, roomJIDStr string) (base, nick string) {
	var explicit, autoNick, autoBase string
	if x.conn != nil && x.conn.br != nil {
		portalKey := networkid.PortalKey{ID: networkid.PortalID(roomJIDStr), Receiver: x.loginID}
		if portal, err := x.conn.br.GetPortalByKey(ctx, portalKey); err == nil && portal != nil {
			if meta, _ := portal.Metadata.(*PortalMetadata); meta != nil {
				explicit, autoNick, autoBase = meta.MUCPreferredNick, meta.MUCAutoNick, meta.MUCAutoNickBase
			}
		}
	}
	var def string
	if x.login != nil {
		if meta, _ := x.login.Metadata.(*JabberLoginMeta); meta != nil {
			def = meta.DefaultMUCNick
		}
	}
	return chooseMUCNick(explicit, x.bookmarkNick(roomJIDStr), def, string(x.loginID), autoNick, autoBase)
}

// startMUCJoinTrack registers a fresh join chain and marks nick as pending. hist is the history
// request of the join (nil = none); 409 retries of the chain send the same one.
func (x *xmppClient) startMUCJoinTrack(roomBare, base, nick string, hist *mucHistory, writeBookmark bool) {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	if x.mucPendingJoin == nil {
		x.mucPendingJoin = make(map[string]string)
	}
	if x.mucJoinTracks == nil {
		x.mucJoinTracks = make(map[string]*mucJoinTrack)
	}
	// A join command whose join is still unconfirmed keeps its bookmark write when a send or view
	// re-sends the join presence before the confirmation arrives.
	if old := x.mucJoinTracks[roomBare]; old != nil && old.writeBookmark {
		writeBookmark = true
	}
	x.mucPendingJoin[roomBare] = nick
	x.mucJoinTracks[roomBare] = &mucJoinTrack{base: base, hist: hist, writeBookmark: writeBookmark}
}

// resetMUCJoinState drops every pending join and retry chain and starts a new generation. Called
// on each new session: join state belongs to the session that sent the join presence, and a stale
// retry goroutine must not send on a newer one (see continueMUCJoinAfterConflict).
func (x *xmppClient) resetMUCJoinState() {
	x.mucPendingJoinMu.Lock()
	x.mucPendingJoin = nil
	x.mucJoinTracks = nil
	x.mucJoinWant = nil
	x.mucJoinGen++
	x.mucPendingJoinMu.Unlock()
	// A new c2s session is in no room: the occupant JIDs of the previous session are stale (the rooms
	// dropped us with the old session). Joins of this session record them again on confirmation.
	x.mucOurOccupantJIDMu.Lock()
	x.mucOurOccupantJID = nil
	x.mucOurOccupantJIDMu.Unlock()
}

// mucJoinChainCurrent reports whether a retry chain started in generation gen is still the one in
// flight: same session, and (when next is set) next is still the pending nick for the room.
func (x *xmppClient) mucJoinChainCurrent(roomBare, next string, gen uint64) bool {
	_, ok := x.mucJoinChainHist(roomBare, next, gen)
	return ok
}

// mucJoinChainHist is mucJoinChainCurrent that also returns the chain's history request.
func (x *xmppClient) mucJoinChainHist(roomBare, next string, gen uint64) (*mucHistory, bool) {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	if x.mucJoinGen != gen {
		return nil, false
	}
	if next != "" && x.mucPendingJoin[roomBare] != next {
		return nil, false
	}
	var hist *mucHistory
	if t := x.mucJoinTracks[roomBare]; t != nil {
		hist = t.hist
	}
	return hist, true
}

// dropMUCJoinTrack forgets the pending join and chain for a room.
func (x *xmppClient) dropMUCJoinTrack(roomBare string) {
	x.mucPendingJoinMu.Lock()
	delete(x.mucPendingJoin, roomBare)
	delete(x.mucJoinTracks, roomBare)
	x.mucPendingJoinMu.Unlock()
}

// onMUCJoinConflict advances the chain after a 409 for ourNick. handled is false when the error does
// not belong to the join in flight (stale or duplicate). Otherwise next is the alternative nick to try
// (now pending), or empty when all are exhausted, in which case tried lists every nick that failed.
// gen is the session generation the chain belongs to.
// Pure state change: safe on the Serve goroutine.
func (x *xmppClient) onMUCJoinConflict(roomBare, ourNick string) (next string, tried []string, gen uint64, handled bool) {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	gen = x.mucJoinGen
	track := x.mucJoinTracks[roomBare]
	if track == nil || x.mucPendingJoin[roomBare] != ourNick {
		return "", nil, gen, false
	}
	track.tried = append(track.tried, ourNick)
	track.conflicted = true
	delete(x.mucPendingJoin, roomBare)
	for i := 1; i <= mucMaxNickAlternatives; i++ {
		cand := fmt.Sprintf("%s_%d", track.base, i)
		taken := false
		for _, t := range track.tried {
			if t == cand {
				taken = true
				break
			}
		}
		if !taken {
			x.mucPendingJoin[roomBare] = cand
			return cand, append([]string(nil), track.tried...), gen, true
		}
	}
	tried = append([]string(nil), track.tried...)
	delete(x.mucJoinTracks, roomBare)
	return "", tried, gen, true
}

// takeMUCJoinTrackLocked removes and returns the chain of a confirmed join. Caller holds mucPendingJoinMu.
func (x *xmppClient) takeMUCJoinTrackLocked(roomBare string) *mucJoinTrack {
	t := x.mucJoinTracks[roomBare]
	delete(x.mucJoinTracks, roomBare)
	return t
}

// continueMUCJoinAfterConflict runs in its own goroutine (never on the Serve goroutine): it sends the
// join with the next alternative nick, or reports that every nick is taken.
func (x *xmppClient) continueMUCJoinAfterConflict(roomBare, next string, tried []string, gen uint64) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.join_retry", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomBare), attribute.Int("xmpp.muc.nick_attempts", len(tried)))
	}
	hist, current := x.mucJoinChainHist(roomBare, next, gen)
	if !current {
		log.Debug().Str("room", roomBare).Msg("MUC 409 retry: session changed or chain superseded, dropped")
		return
	}
	if next == "" {
		x.dropMUCJoinWant(roomBare) // every nick refused: the watchdog must not keep retrying
		log.Warn().Str("room", roomBare).Strs("tried", tried).Msg("MUC join failed: all nicks taken")
		x.notifyMUC(ctx, roomBare, mucAllTakenNotice(x.mucCmdPrefix(), roomBare, tried))
		return
	}
	if err := x.sendMUCJoin(ctx, roomBare, next, hist); err != nil {
		x.dropMUCJoinTrack(roomBare)
		telemetry.Fail(span, err)
		log.Warn().Err(err).Str("room", roomBare).Str("nick", next).Msg("MUC 409 retry: send failed")
		return
	}
	log.Info().Str("room", roomBare).Str("nick", next).Msg("MUC 409 retry: sent join with alternative nick")
}

// announceMUCNickChange runs in its own goroutine once a join with an alternative nick is confirmed:
// remember the nick for rejoins and tell the user.
func (x *xmppClient) announceMUCNickChange(roomBare, base, taken, nick string) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.nick_notice", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomBare))
	}
	if err := x.rememberAutoMUCNick(ctx, roomBare, base, nick); err != nil {
		telemetry.Fail(span, err)
		log.Warn().Err(err).Str("room", roomBare).Msg("Failed to remember alternative MUC nick")
	}
	x.notifyMUC(ctx, roomBare, mucNickChangedNotice(x.mucCmdPrefix(), roomBare, taken, nick))
}

// rememberAutoMUCNick stores a working alternative nick in the portal metadata (separate from the
// user-set MUCPreferredNick, see chooseMUCNick).
func (x *xmppClient) rememberAutoMUCNick(ctx context.Context, roomBare, base, nick string) error {
	if x.mucNickSaveHook != nil {
		return x.mucNickSaveHook(roomBare, base, nick)
	}
	if x.conn == nil || x.conn.br == nil {
		return fmt.Errorf("no bridge")
	}
	portal, err := x.conn.br.GetPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID})
	if err != nil {
		return err
	}
	meta, _ := portal.Metadata.(*PortalMetadata)
	if meta == nil {
		meta = &PortalMetadata{}
	}
	meta.MUCAutoNick = nick
	meta.MUCAutoNickBase = base
	portal.Metadata = meta
	return portal.Save(ctx)
}

// notifyMUC posts an m.notice from the bridge bot to the room's portal (when it has a Matrix room)
// and to the user's management room.
func (x *xmppClient) notifyMUC(ctx context.Context, roomBare, text string) {
	if x.mucNoticeHook != nil {
		x.mucNoticeHook(roomBare, text)
		return
	}
	if x.conn == nil || x.conn.br == nil || x.conn.br.Bot == nil {
		return
	}
	var targets []id.RoomID
	if portal, err := x.conn.br.GetPortalByKey(ctx, networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID}); err == nil && portal != nil && portal.MXID != "" {
		targets = append(targets, portal.MXID)
	}
	if x.login != nil && x.login.User != nil && x.login.User.ManagementRoom != "" {
		targets = append(targets, x.login.User.ManagementRoom)
	}
	content := format.RenderMarkdown(text, true, false)
	content.MsgType = event.MsgNotice
	for _, rid := range targets {
		c := content
		if _, err := x.conn.br.Bot.SendMessage(ctx, rid, event.EventMessage, &event.Content{Parsed: &c}, nil); err != nil {
			x.logFor(ctx).Warn().Err(err).Msg("Failed to send MUC nick notice")
		}
	}
}

// mucCmdPrefix returns the configured bot command prefix.
func (x *xmppClient) mucCmdPrefix() string {
	if x.conn != nil && x.conn.br != nil && x.conn.br.Config != nil && x.conn.br.Config.CommandPrefix != "" {
		return x.conn.br.Config.CommandPrefix
	}
	return "!jabber"
}

// mucNickChangedNotice words the notice around taken, the first nick actually tried in the chain
// (the preferred nick, or a remembered alternative that is now taken as well).
func mucNickChangedNotice(prefix, room, taken, nick string) string {
	return fmt.Sprintf("`%s` is taken in %s, joined as `%s`. Use `%s muc-nick %s <nick>` to pick another.", taken, room, nick, prefix, room)
}

func mucAllTakenNotice(prefix, room string, tried []string) string {
	quoted := make([]string, len(tried))
	for i, t := range tried {
		quoted[i] = "`" + t + "`"
	}
	return fmt.Sprintf("Could not join %s: nicks %s are all taken. Use `%s muc-nick %s <nick>` and `%s join %s` to try again.",
		room, strings.Join(quoted, ", "), prefix, room, prefix, room)
}

// noteOwnMUCExit forgets the occupant JID recorded for roomBare when occupant is it (or the presence
// carries the self-presence status 110): the room no longer counts us as an occupant (kick, ban,
// affiliation or members-only removal, shutdown, destroy, or a presence error from our occupant JID).
// The next send or view then joins again. Serve goroutine, memory only.
func (x *xmppClient) noteOwnMUCExit(roomBare, occupant string, info presenceInfo, why string) bool {
	x.mucOurOccupantJIDMu.Lock()
	ours := x.mucOurOccupantJID[roomBare]
	if ours == "" || (ours != occupant && !info.hasStatus("110")) {
		x.mucOurOccupantJIDMu.Unlock()
		return false
	}
	delete(x.mucOurOccupantJID, roomBare)
	x.mucOurOccupantJIDMu.Unlock()
	x.log.Info().Str("room", roomBare).Str("reason", why).Strs("status", info.statusCodes).Msg("No longer in MUC room, will re-join on next send or view")
	return true
}
