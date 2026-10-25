package connector

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// nickChangeOrigin says who asked for a nick change (wording and side effects of the result).
type nickChangeOrigin int

const (
	nickByCommand nickChangeOrigin = iota // muc-nick
	nickByJoin                            // join on a joined room whose nick choice changed
	nickBySync                            // Matrix per-room display name (muc_nick_sync)
)

func (o nickChangeOrigin) String() string {
	switch o {
	case nickByCommand:
		return "command"
	case nickByJoin:
		return "join"
	case nickBySync:
		return "sync"
	}
	return "unknown"
}

// nickResult is the outcome of a nick change.
type nickResult int

const (
	nickSuccess nickResult = iota
	nickConflict
	nickError
	nickTimeout
)

func (r nickResult) String() string {
	switch r {
	case nickSuccess:
		return "success"
	case nickConflict:
		return "conflict"
	case nickError:
		return "error"
	}
	return "timeout"
}

// mucNickChange is the one nick change in flight for a room (xmppClient.mucNickChanges).
type mucNickChange struct {
	want          string // requested nick (as sent in the presence)
	old           string // nick held when it was sent
	origin        nickChangeOrigin
	persist       bool   // store want as the explicit muc-nick once confirmed (command, sync)
	writeBookmark bool   // join: write the bookmark after the result
	base          string // nick-choice base at send time (bookmark nick offer)
	// sync only: revert target and the display name that triggered it (never logged)
	prevDisplay, display string
	portalMXID           id.RoomID
	gen                  uint64
	timer                *time.Timer
}

// pastOccupant is an occupant JID we held earlier in a room.
type pastOccupant struct {
	jid   string
	until time.Time // echo matching window end (now + mucPastOwnEchoTTL)
}

var (
	// mucNickChangeTimeout is how long a nick change waits for the room's answer (tests shorten it).
	mucNickChangeTimeout = 30 * time.Second
	// mucPastOwnEchoTTL is how long the echo of a message sent just before a rename is still matched.
	mucPastOwnEchoTTL = 10 * time.Minute
)

const (
	mucNickMaxRunes = 64
	mucPastOwnMax   = 8
)

// sanitizeMUCNick cleans a user-supplied nick for roomBare: control and format characters are dropped,
// whitespace runs become one space, the result is trimmed and cut to mucNickMaxRunes runes. The returned
// nick is the PRECIS-normalized resource part; reason is non-empty (and nick empty) when it cannot be used.
func sanitizeMUCNick(roomBare, raw string) (nick string, reason string) {
	var b strings.Builder
	space := false
	for _, r := range raw {
		switch {
		case unicode.IsSpace(r):
			space = true
			continue
		case unicode.Is(unicode.Cc, r) || unicode.Is(unicode.Cf, r):
			continue
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
		}
		space = false
		b.WriteRune(r)
	}
	nick = strings.TrimSpace(b.String())
	if rs := []rune(nick); len(rs) > mucNickMaxRunes {
		nick = strings.TrimSpace(string(rs[:mucNickMaxRunes]))
	}
	if nick == "" {
		return "", "empty"
	}
	room, err := jid.Parse(roomBare)
	if err != nil {
		return "", "invalid characters"
	}
	full, err := room.Bare().WithResource(nick)
	if err != nil {
		return "", "invalid characters"
	}
	return full.Resourcepart(), ""
}

// noticeNick renders a nick as inline code (nicks are user-controlled Markdown otherwise).
func noticeNick(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "'") + "`"
}

func mucNickChangedOKNotice(room, nick string) string {
	return fmt.Sprintf("Your nick in %s is now %s.", room, noticeNick(nick))
}

func mucNickAdjustedNotice(room, want, nick string) string {
	return fmt.Sprintf("Your nick in %s is now %s (the room adjusted %s).", room, noticeNick(nick), noticeNick(want))
}

func mucNickTakenNotice(prefix, room, want, old string) string {
	return fmt.Sprintf("%s is taken in %s; you are still %s. Use `%s muc-nick %s <nick>` to pick another.", noticeNick(want), room, noticeNick(old), prefix, room)
}

func mucNickRefusedNotice(room, want, old, cond string) string {
	return fmt.Sprintf("Could not change your nick in %s to %s (%s); you are still %s.", room, noticeNick(want), cond, noticeNick(old))
}

func mucNickTimeoutNotice(room, want, old string) string {
	return fmt.Sprintf("%s did not answer the nick change to %s; as far as the bridge knows you are still %s.", room, noticeNick(want), noticeNick(old))
}

// mucNickSyncFailedNotice is the sync-origin notice (portal room only); reverted says whether the room
// display name was put back.
func mucNickSyncFailedNotice(want, old, why string, reverted bool) string {
	s := fmt.Sprintf("Could not use %s as your nick in this XMPP room (%s); you are still %s there.", noticeNick(want), why, noticeNick(old))
	if reverted {
		s += " Your display name in this room was set back."
	}
	return s
}

func mucNickSyncInvalidNotice(old, reason string) string {
	return fmt.Sprintf("Your display name in this room cannot be used as an XMPP nick (%s); your nick stays %s.", reason, noticeNick(old))
}

// sendMUCNickChange sends the XEP-0045 7.6 nick change: a plain presence to room/newnick. It carries no
// MUC <x/> (some servers treat that as a re-join and replay the history), no password and no history.
func (x *xmppClient) sendMUCNickChange(ctx context.Context, roomBare, nick string) error {
	if x.mucNickChangeHook != nil {
		return x.mucNickChangeHook(roomBare, nick)
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("not connected to XMPP")
	}
	roomJID, err := jid.Parse(roomBare)
	if err != nil {
		return fmt.Errorf("invalid MUC JID %q: %w", roomBare, err)
	}
	toJID, err := roomJID.WithResource(nick)
	if err != nil {
		return fmt.Errorf("MUC JID with resource: %w", err)
	}
	if err := sess.Send(ctx, stanza.Presence{To: toJID}.Wrap(nil)); err != nil {
		return fmt.Errorf("send MUC nick change presence: %w", err)
	}
	return nil
}

// startMUCNickChange records ch as the nick change in flight for roomBare (superseding an older one),
// arms its timeout and sends the nick-change presence. A send error removes the record again.
func (x *xmppClient) startMUCNickChange(ctx context.Context, roomBare string, ch *mucNickChange) error {
	ctx, span := telemetry.Start(ctx, "xmpp.muc.nick_change", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomBare))
	}
	log := x.logFor(ctx)
	x.mucPendingJoinMu.Lock()
	ch.gen = x.mucJoinGen
	if x.mucNickChanges == nil {
		x.mucNickChanges = make(map[string]*mucNickChange)
	}
	if old := x.mucNickChanges[roomBare]; old != nil {
		if old.timer != nil {
			old.timer.Stop()
		}
		log.Debug().Str("room", roomBare).Msg("MUC nick change superseded by a newer one")
	}
	ch.timer = time.AfterFunc(mucNickChangeTimeout, func() { x.onMUCNickChangeTimeout(roomBare, ch) })
	x.mucNickChanges[roomBare] = ch
	x.mucPendingJoinMu.Unlock()

	if err := x.sendMUCNickChange(ctx, roomBare, ch.want); err != nil {
		x.mucPendingJoinMu.Lock()
		if x.mucNickChanges[roomBare] == ch {
			delete(x.mucNickChanges, roomBare)
		}
		x.mucPendingJoinMu.Unlock()
		ch.timer.Stop()
		telemetry.Fail(span, err)
		return err
	}
	if ch.origin == nickBySync {
		// The nick is the user's Matrix display name: lengths only.
		log.Info().Str("room", roomBare).Int("nick_len", len(ch.want)).Str("origin", ch.origin.String()).Msg("Sent MUC nick change presence")
	} else {
		log.Info().Str("room", roomBare).Str("old_nick", ch.old).Str("nick", ch.want).Str("origin", ch.origin.String()).Msg("Sent MUC nick change presence")
	}
	return nil
}

// takeMUCNickChange removes and returns the nick change in flight (any own 303 answers it). Serve-safe.
func (x *xmppClient) takeMUCNickChange(roomBare string) *mucNickChange {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	ch := x.mucNickChanges[roomBare]
	if ch == nil {
		return nil
	}
	delete(x.mucNickChanges, roomBare)
	if ch.timer != nil {
		ch.timer.Stop()
	}
	return ch
}

// takeMUCNickChangeFor is takeMUCNickChange for the error path: only when nick is the requested one.
func (x *xmppClient) takeMUCNickChangeFor(roomBare, nick string) *mucNickChange {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	ch := x.mucNickChanges[roomBare]
	if ch == nil || !(ch.want == nick || strings.EqualFold(ch.want, nick)) {
		return nil
	}
	delete(x.mucNickChanges, roomBare)
	if ch.timer != nil {
		ch.timer.Stop()
	}
	return ch
}

// dropMUCNickChange silently forgets the nick change in flight (leave, re-join).
func (x *xmppClient) dropMUCNickChange(roomBare string) {
	x.takeMUCNickChange(roomBare)
}

// onMUCNickChangeTimeout runs on the timer goroutine.
func (x *xmppClient) onMUCNickChangeTimeout(roomBare string, ch *mucNickChange) {
	x.mucPendingJoinMu.Lock()
	if x.mucNickChanges[roomBare] != ch || ch.gen != x.mucJoinGen {
		x.mucPendingJoinMu.Unlock()
		return
	}
	delete(x.mucNickChanges, roomBare)
	x.mucPendingJoinMu.Unlock()
	x.finishMUCNickChange(roomBare, ch, nickTimeout, "", "")
}

// finishMUCNickChange applies the result of a nick change: persistence, bookmark, notices and the Matrix
// display name. Never call it on the Serve goroutine (callers use `go`).
func (x *xmppClient) finishMUCNickChange(roomBare string, ch *mucNickChange, res nickResult, actual, cond string) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.muc.nick_change_result", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomBare), attribute.String("xmpp.muc.nick_result", res.String()))
		if cond != "" {
			span.SetAttributes(attribute.String("xmpp.error.condition", cond))
		}
	}
	log := x.logFor(ctx)
	prefix := x.mucCmdPrefix()
	notify := ch.origin != nickBySync

	if res == nickSuccess {
		if actual == "" {
			actual = ch.want
		}
		ev := log.Info().Str("room", roomBare).Str("origin", ch.origin.String())
		if ch.origin == nickBySync {
			ev = ev.Int("nick_len", len(actual))
		} else {
			ev = ev.Str("nick", actual)
		}
		ev.Msg("MUC nick change confirmed")
		if ch.persist {
			if err := x.saveExplicitMUCNick(ctx, roomBare, ch.want); err != nil {
				telemetry.Fail(span, err)
				log.Warn().Err(err).Str("room", roomBare).Msg("Failed to store the new MUC nick")
			}
		}
		if ch.writeBookmark {
			nick := actual
			if actual != ch.base {
				nick = ""
			}
			x.publishJoinBookmark(ctx, roomBare, nick)
		}
		if notify {
			if actual != ch.want {
				x.notifyMUC(ctx, roomBare, mucNickAdjustedNotice(roomBare, ch.want, actual))
			} else {
				x.notifyMUC(ctx, roomBare, mucNickChangedOKNotice(roomBare, actual))
			}
		}
		x.syncOwnNickToMatrix(ctx, roomBare, actual)
		return
	}

	ev := log.Warn().Str("room", roomBare).Str("result", res.String()).Str("condition", cond)
	if ch.origin == nickBySync {
		ev = ev.Int("nick_len", len(ch.want))
	} else {
		ev = ev.Str("nick", ch.want)
	}
	ev.Msg("MUC nick change failed")
	if ch.writeBookmark {
		x.publishJoinBookmark(ctx, roomBare, "")
	}
	switch ch.origin {
	case nickBySync:
		why := cond
		switch res {
		case nickConflict:
			why = "it is taken"
		case nickTimeout:
			why = "the room did not answer"
		}
		if why == "" {
			why = "refused"
		}
		reverted := x.revertOwnRoomDisplayName(ctx, ch)
		x.notifyPortalRoom(ctx, roomBare, ch.portalMXID, mucNickSyncFailedNotice(ch.want, ch.old, why, reverted))
	default:
		var text string
		switch res {
		case nickConflict:
			text = mucNickTakenNotice(prefix, roomBare, ch.want, ch.old)
		case nickTimeout:
			text = mucNickTimeoutNotice(roomBare, ch.want, ch.old)
		default:
			if cond == "" {
				cond = "refused"
			}
			text = mucNickRefusedNotice(roomBare, ch.want, ch.old, cond)
		}
		x.notifyMUC(ctx, roomBare, text)
	}
}

// saveExplicitMUCNick stores nick as the room's explicit (muc-nick) nick.
func (x *xmppClient) saveExplicitMUCNick(ctx context.Context, roomBare, nick string) error {
	if x.mucExplicitSaveHook != nil {
		return x.mucExplicitSaveHook(roomBare, nick)
	}
	if x.conn == nil || x.conn.br == nil {
		return fmt.Errorf("no bridge")
	}
	return saveExplicitMUCNickOn(ctx, x.conn.br, networkid.PortalKey{ID: networkid.PortalID(roomBare), Receiver: x.loginID}, nick)
}

// noteMUCPastOwnLocked remembers an occupant JID we held. Caller holds mucOurOccupantJIDMu.
func (x *xmppClient) noteMUCPastOwnLocked(roomBare, occupant string, now time.Time) {
	if occupant == "" {
		return
	}
	list := x.mucPastOwn[roomBare]
	kept := list[:0:0]
	for _, p := range list {
		if p.jid != occupant {
			kept = append(kept, p)
		}
	}
	kept = append(kept, pastOccupant{jid: occupant, until: now.Add(mucPastOwnEchoTTL)})
	if len(kept) > mucPastOwnMax {
		kept = kept[len(kept)-mucPastOwnMax:]
	}
	if x.mucPastOwn == nil {
		x.mucPastOwn = make(map[string][]pastOccupant)
	}
	x.mucPastOwn[roomBare] = kept
}

// wasOwnMUCOccupant reports whether occupant is a JID we held earlier this session in the room.
func (x *xmppClient) wasOwnMUCOccupant(roomBare, occupant string) bool {
	x.mucOurOccupantJIDMu.RLock()
	defer x.mucOurOccupantJIDMu.RUnlock()
	for _, p := range x.mucPastOwn[roomBare] {
		if p.jid == occupant {
			return true
		}
	}
	return false
}

// wasOwnMUCOccupantForEcho is wasOwnMUCOccupant limited to the echo window.
func (x *xmppClient) wasOwnMUCOccupantForEcho(roomBare, occupant string, now time.Time) bool {
	x.mucOurOccupantJIDMu.RLock()
	defer x.mucOurOccupantJIDMu.RUnlock()
	for _, p := range x.mucPastOwn[roomBare] {
		if p.jid == occupant && p.until.After(now) {
			return true
		}
	}
	return false
}

// SetMUCNick is the muc-nick command (MUCNickChanger): when the bridge is in the room the nick changes
// at once (the result follows as a notice, and only a confirmed change is stored); otherwise the nick is
// stored and used at the next join.
func (x *xmppClient) SetMUCNick(ctx context.Context, room, nick string) (MUCNickStatus, string, error) {
	clean, reason := sanitizeMUCNick(room, nick)
	if reason != "" {
		return MUCNickSaved, "", fmt.Errorf("invalid nick: %s", reason)
	}
	nick = clean
	occ := x.getMUCOurOccupantJID(room)
	if occ == "" || x.isMUCLeft(ctx, room) {
		return MUCNickSaved, "", x.saveExplicitMUCNick(ctx, room, nick)
	}
	cur := resourceOf(occ)
	if cur == nick {
		return MUCNickUnchanged, cur, x.saveExplicitMUCNick(ctx, room, nick)
	}
	err := x.startMUCNickChange(ctx, room, &mucNickChange{want: nick, old: cur, origin: nickByCommand, persist: true})
	return MUCNickChanging, cur, err
}

var (
	_ MUCNickChanger  = (*xmppClient)(nil)
	_ MUCJoinReporter = (*xmppClient)(nil)
)
