package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// MUC join health: classification and logging of join errors, a watchdog that re-sends a join that was
// never confirmed, and an XEP-0410 self-ping of joined rooms. All state is per session and in memory.

// Timing (variables so tests can shorten them).
var (
	mucWatchdogTick       = 15 * time.Second // watchdog tick
	mucJoinConfirmTimeout = 60 * time.Second // how long a join may stay unconfirmed before it is re-sent
	mucJoinRetryBase      = 30 * time.Second // extra wait added after the first unconfirmed attempt, doubled per attempt
	mucJoinRetryMax       = 10 * time.Minute // cap of the extra wait
	mucJoinNoticeAfter    = 5                // attempts without confirmation before the "still trying" notice
	mucSelfPingInterval   = 15 * time.Minute // self-ping period per joined room
)

// mucSelfPingPerTick bounds the self-pings of one watchdog tick.
const mucSelfPingPerTick = 5

// mucJoinWant is a room this session sent a join for and still wants to be in (xmppClient.mucJoinWant,
// guarded by mucPendingJoinMu).
type mucJoinWant struct {
	attempts      int       // join presences sent without a confirmation (1 after the first send)
	deadline      time.Time // when the watchdog re-sends the join if still unconfirmed
	nickOverride  string    // nickOverride of the original joinMUCNick call ("" = nick choice)
	writeBookmark bool      // the join command: retries keep the bookmark write
	noticed       bool      // the "still trying" notice was posted
}

// mucJoinErrorPermanent reports whether a join error condition is a refusal that retrying cannot fix
// (banned, members-only, password, no such room, creation refused, bad JID). Everything else
// (remote-server-timeout, service-unavailable, internal-server-error, unknown, ...) is transient.
func mucJoinErrorPermanent(cond string) bool {
	switch cond {
	case "forbidden", "registration-required", "not-authorized", "item-not-found", "not-allowed", "jid-malformed", "gone":
		return true
	}
	return false
}

// mucJoinRetryDelay is the extra wait after n unconfirmed attempts: 0 for n <= 0, else
// mucJoinRetryBase << (n-1), capped at mucJoinRetryMax.
func mucJoinRetryDelay(n int) time.Duration {
	if n <= 0 {
		return 0
	}
	d := mucJoinRetryBase
	for i := 1; i < n; i++ {
		if d >= mucJoinRetryMax {
			break
		}
		d *= 2
	}
	if d > mucJoinRetryMax {
		d = mucJoinRetryMax
	}
	return d
}

// mucJoinRefusedNotice words the notice for a permanently refused join.
func mucJoinRefusedNotice(prefix, room, cond string) string {
	return fmt.Sprintf("Could not join %s: the room refused the join (%s). Use `%s join %s` to try again.", room, cond, prefix, room)
}

// mucJoinStuckNotice words the notice for a join that the room does not answer.
func mucJoinStuckNotice(room string) string {
	return fmt.Sprintf("Still trying to re-join %s: the room has not answered. The bridge keeps retrying in the background.", room)
}

// noteMUCJoinSent records a join presence for room that is about to be sent: creates or updates the want
// entry, counts the attempt and sets the confirmation deadline. writeBookmark is sticky. It runs before
// the send so a fast confirmation or refusal (handled on the Serve goroutine) always finds the entry;
// created reports a new entry, which the caller drops again when the send fails.
func (x *xmppClient) noteMUCJoinSent(room, nickOverride string, writeBookmark bool, now time.Time) (created bool) {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	if x.mucJoinWant == nil {
		x.mucJoinWant = make(map[string]*mucJoinWant)
	}
	w := x.mucJoinWant[room]
	if w == nil {
		w = &mucJoinWant{}
		x.mucJoinWant[room] = w
		created = true
	}
	w.attempts++
	w.deadline = now.Add(mucJoinConfirmTimeout + mucJoinRetryDelay(w.attempts-1))
	w.nickOverride = nickOverride
	w.writeBookmark = w.writeBookmark || writeBookmark
	return created
}

// noteMUCJoinConfirmedLocked drops the want entry of a confirmed join. Caller holds mucPendingJoinMu.
func (x *xmppClient) noteMUCJoinConfirmedLocked(room string) {
	w := x.mucJoinWant[room]
	if w == nil {
		return
	}
	delete(x.mucJoinWant, room)
	if w.attempts > 1 {
		x.log.Info().Str("room", room).Int("attempts", w.attempts).Msg("MUC join confirmed after retry")
	}
}

// dropMUCJoinWant forgets the want entry of a room (leave, permanent failure).
func (x *xmppClient) dropMUCJoinWant(room string) {
	x.mucPendingJoinMu.Lock()
	delete(x.mucJoinWant, room)
	x.mucPendingJoinMu.Unlock()
}

// onMUCJoinError handles a presence error that is not a nick conflict. It acts only for a join in
// flight (pending, and ourNick empty or the pending nick): it clears the pending join and chain, logs
// one line (never the server's error text) and either drops the want entry and reports a permanent
// refusal, or schedules the watchdog retry. Serve goroutine: state only. Returns whether it acted.
func (x *xmppClient) onMUCJoinError(room, ourNick, errType, cond string) bool {
	permanent := mucJoinErrorPermanent(cond)
	x.mucPendingJoinMu.Lock()
	pendingNick, pending := x.mucPendingJoin[room]
	if !pending || (ourNick != "" && ourNick != pendingNick) {
		x.mucPendingJoinMu.Unlock()
		x.log.Debug().Str("room", room).Str("condition", cond).Msg("MUC presence error without a matching pending join")
		return false
	}
	delete(x.mucPendingJoin, room)
	delete(x.mucJoinTracks, room)
	if w := x.mucJoinWant[room]; w != nil {
		if permanent {
			delete(x.mucJoinWant, room)
		} else {
			w.deadline = time.Now().Add(mucJoinRetryDelay(w.attempts))
		}
	}
	x.mucPendingJoinMu.Unlock()
	x.log.Warn().Str("room", room).Str("nick", pendingNick).Str("condition", cond).Str("error_type", errType).
		Bool("permanent", permanent).Msg("MUC join failed")
	if permanent {
		go x.reportMUCJoinRefused(room, cond)
	}
	return true
}

// reportMUCJoinRefused runs in its own goroutine: it tells the user a join was refused for good.
func (x *xmppClient) reportMUCJoinRefused(room, cond string) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.join_refused", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", room), attribute.String("xmpp.error.condition", cond))
	}
	x.notifyMUC(ctx, room, mucJoinRefusedNotice(x.mucCmdPrefix(), room, cond))
}

// mucJoinGeneration returns the current session generation of the join state.
func (x *xmppClient) mucJoinGeneration() uint64 {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	return x.mucJoinGen
}

// dueJoin is one overdue join of a watchdog tick.
type dueJoin struct {
	room string
	mucJoinWant
}

// dueMUCJoinRetries returns the want entries whose deadline passed in generation gen and pushes their
// deadline to now + mucJoinConfirmTimeout, so a slow tick never double-sends.
func (x *xmppClient) dueMUCJoinRetries(gen uint64, now time.Time) []dueJoin {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	if x.mucJoinGen != gen {
		return nil
	}
	var due []dueJoin
	for room, w := range x.mucJoinWant {
		if now.Before(w.deadline) {
			continue
		}
		due = append(due, dueJoin{room: room, mucJoinWant: *w})
		w.deadline = now.Add(mucJoinConfirmTimeout)
	}
	return due
}

// stuckMUCJoins returns (once per room) the rooms whose join reached mucJoinNoticeAfter attempts.
func (x *xmppClient) stuckMUCJoins(gen uint64) []string {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	if x.mucJoinGen != gen {
		return nil
	}
	var rooms []string
	for room, w := range x.mucJoinWant {
		if w.attempts >= mucJoinNoticeAfter && !w.noticed {
			w.noticed = true
			rooms = append(rooms, room)
		}
	}
	return rooms
}

// runMUCJoinHealth is the per-session watchdog goroutine (started by Connect): every mucWatchdogTick
// it re-sends overdue joins and self-pings joined rooms. It ends with ctx or when a new session
// started (generation changed). Never runs on the Serve goroutine.
func (x *xmppClient) runMUCJoinHealth(ctx context.Context) {
	gen := x.mucJoinGeneration()
	ticker := time.NewTicker(mucWatchdogTick)
	defer ticker.Stop()
	pingAt := make(map[string]time.Time)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if x.mucJoinGeneration() != gen {
			return
		}
		now := time.Now()
		x.mucWatchdogRetries(ctx, gen, now)
		if ctx.Err() != nil {
			return
		}
		x.mucSelfPingTick(ctx, gen, pingAt, now)
	}
}

// mucWatchdogRetries re-sends the overdue joins (staggered) and posts the one-time "still trying"
// notice.
func (x *xmppClient) mucWatchdogRetries(ctx context.Context, gen uint64, now time.Time) {
	for i, d := range x.dueMUCJoinRetries(gen, now) {
		if i > 0 && !waitCtx(ctx, mucRejoinInterval) {
			return
		}
		x.retryMUCJoin(ctx, gen, d, now)
	}
	for _, room := range x.stuckMUCJoins(gen) {
		x.notifyMUC(ctx, room, mucJoinStuckNotice(room))
	}
}

// retryMUCJoin re-sends one overdue join with its own root span, unless the room is confirmed or left.
func (x *xmppClient) retryMUCJoin(ctx context.Context, gen uint64, d dueJoin, now time.Time) {
	if ctx.Err() != nil || x.mucJoinGeneration() != gen {
		return // the session ended: never send on a newer one
	}
	if x.getMUCOurOccupantJID(d.room) != "" || x.isMUCLeft(ctx, d.room) {
		x.dropMUCJoinWant(d.room)
		return
	}
	ctx, span := telemetry.Start(ctx, "xmpp.muc.join_watchdog", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", d.room), attribute.Int("xmpp.muc.join_attempt", d.attempts))
	}
	log := x.logFor(ctx)
	log.Warn().Str("room", d.room).Int("attempt", d.attempts).Msg("MUC join not confirmed, retrying")
	hist := mucRejoinHistory(x.newestBridgedMessage(ctx, d.room), now)
	if err := x.joinMUCNick(ctx, d.room, hist, d.writeBookmark, d.nickOverride); err != nil {
		telemetry.Fail(span, err)
		log.Warn().Err(err).Str("room", d.room).Msg("MUC join watchdog: re-send failed")
	}
}

// selfPingResult is the verdict of a MUC self-ping.
type selfPingResult int

const (
	selfPingInconclusive selfPingResult = iota // timeout, transport or other error: ping again later
	selfPingJoined                             // we are an occupant
	selfPingNotJoined                          // the room says we are not an occupant
)

// classifySelfPing maps the result of an XEP-0410 ping to our own occupant JID: success,
// service-unavailable/feature-not-implemented (our occupant's client lacks ping) or item-not-found (we
// are joined, but the occupant was just renamed, e.g. by another client) means joined (XEP-0410 3);
// not-acceptable (what Prosody and ejabberd answer a non-occupant) means not an occupant; anything else
// (timeouts, remote-server-*, other errors) is inconclusive, so a doubtful answer never floods a rejoin.
func classifySelfPing(err error) selfPingResult {
	if err == nil {
		return selfPingJoined
	}
	var se stanza.Error
	if !errors.As(err, &se) {
		return selfPingInconclusive
	}
	switch string(se.Condition) {
	case "service-unavailable", "feature-not-implemented", "item-not-found":
		return selfPingJoined
	case "not-acceptable":
		return selfPingNotJoined
	}
	return selfPingInconclusive
}

// mucSelfPingTick pings (at most mucSelfPingPerTick) joined rooms whose last self-ping is older than
// mucSelfPingInterval; pingAt is owned by the caller's goroutine. The first sight of a room only
// records now. A room that answers "not an occupant" is re-joined with the nick we held.
func (x *xmppClient) mucSelfPingTick(ctx context.Context, gen uint64, pingAt map[string]time.Time, now time.Time) {
	x.mucOurOccupantJIDMu.RLock()
	occupants := make(map[string]string, len(x.mucOurOccupantJID))
	for room, occ := range x.mucOurOccupantJID {
		occupants[room] = occ
	}
	x.mucOurOccupantJIDMu.RUnlock()
	for room := range pingAt {
		if occupants[room] == "" {
			delete(pingAt, room)
		}
	}
	pinged := 0
	for room, occ := range occupants {
		last, seen := pingAt[room]
		if !seen {
			pingAt[room] = now
			continue
		}
		if now.Sub(last) < mucSelfPingInterval {
			continue
		}
		if pinged >= mucSelfPingPerTick || ctx.Err() != nil {
			return
		}
		pinged++
		pingAt[room] = now
		x.selfPingRoom(ctx, gen, room, occ, now)
	}
}

// selfPingRoom pings one occupant JID and acts on the verdict. Successful pings leave no span or log.
func (x *xmppClient) selfPingRoom(ctx context.Context, gen uint64, room, occupant string, now time.Time) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil && x.pingFunc == nil {
		return
	}
	to, err := jid.Parse(occupant)
	if err != nil {
		return
	}
	perr := x.sendPing(ctx, sess, to, routePing)
	if ctx.Err() != nil || x.mucJoinGeneration() != gen {
		return
	}
	switch classifySelfPing(perr) {
	case selfPingJoined:
		return
	case selfPingInconclusive:
		// Log the condition only: stanza.Error.Error() returns the server's <text>.
		var se stanza.Error
		if errors.As(perr, &se) {
			x.log.Debug().Str("condition", string(se.Condition)).Str("room", room).Msg("MUC self-ping inconclusive")
		} else {
			x.log.Debug().Err(perr).Str("room", room).Msg("MUC self-ping inconclusive")
		}
		return
	}
	// Not an occupant: forget the occupant JID if it is still the one we pinged, then re-join.
	x.mucOurOccupantJIDMu.Lock()
	if x.mucOurOccupantJID[room] != occupant {
		x.mucOurOccupantJIDMu.Unlock()
		return
	}
	delete(x.mucOurOccupantJID, room)
	x.mucOurOccupantJIDMu.Unlock()
	if x.isMUCLeft(ctx, room) || ctx.Err() != nil || x.mucJoinGeneration() != gen {
		return
	}
	ctx, span := telemetry.Start(ctx, "xmpp.muc.self_ping_rejoin", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", room))
	}
	log := x.logFor(ctx)
	log.Info().Str("room", room).Msg("MUC self-ping: not an occupant, re-joining")
	hist := mucRejoinHistory(x.newestBridgedMessage(ctx, room), now)
	if err := x.joinMUCNick(ctx, room, hist, false, resourceOf(occupant)); err != nil {
		telemetry.Fail(span, err)
		log.Warn().Err(err).Str("room", room).Msg("MUC self-ping: re-join failed")
	}
}
