package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/database"
	"mellium.im/xmpp"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"
)

const jhRoom = "room@conference.example.com"

type jhRig struct {
	x       *xmppClient
	mu      sync.Mutex
	sent    []string // nicks
	hists   []*mucHistory
	notices []string
}

func newJHRig(t *testing.T) *jhRig {
	t.Helper()
	r := &jhRig{x: &xmppClient{loginID: "alice@example.com", log: zerolog.Nop()}}
	r.x.mucSendJoinHook = func(room, nick string) error {
		r.mu.Lock()
		r.sent = append(r.sent, nick)
		r.mu.Unlock()
		return nil
	}
	r.x.mucJoinHistHook = func(room string, h *mucHistory) {
		r.mu.Lock()
		r.hists = append(r.hists, h)
		r.mu.Unlock()
	}
	r.x.mucNoticeHook = func(room, text string) {
		r.mu.Lock()
		r.notices = append(r.notices, text)
		r.mu.Unlock()
	}
	r.x.mucNewestMsgHook = func(context.Context, string) time.Time { return time.Time{} }
	r.x.mucRejoinPortalsHook = func(context.Context) ([]*database.Portal, error) { return nil, nil }
	oldTick, oldConfirm, oldBase, oldInt := mucWatchdogTick, mucJoinConfirmTimeout, mucJoinRetryBase, mucRejoinInterval
	t.Cleanup(func() {
		mucWatchdogTick, mucJoinConfirmTimeout, mucJoinRetryBase, mucRejoinInterval = oldTick, oldConfirm, oldBase, oldInt
	})
	mucRejoinInterval = time.Millisecond
	return r
}

func (r *jhRig) sentCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

func (r *jhRig) noticeCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.notices)
}

func (r *jhRig) want(room string) *mucJoinWant {
	r.x.mucPendingJoinMu.Lock()
	defer r.x.mucPendingJoinMu.Unlock()
	if w := r.x.mucJoinWant[room]; w != nil {
		c := *w
		return &c
	}
	return nil
}

func TestParsePresenceJoinError(t *testing.T) {
	const errNS = `xmlns='urn:ietf:params:xml:ns:xmpp-stanzas'`
	p := parseTestPresence(t, `<presence type='error'><error type='wait'><remote-server-timeout `+errNS+`/><text `+errNS+`>SECRET</text></error></presence>`)
	if p.errType != "wait" || p.errCondition != "remote-server-timeout" || p.conflict {
		t.Fatalf("%+v", p)
	}
	p = parseTestPresence(t, `<presence type='error'><error type='auth'><text `+errNS+`>SECRET</text><forbidden `+errNS+`/></error></presence>`)
	if p.errType != "auth" || p.errCondition != "forbidden" {
		t.Fatalf("%+v", p)
	}
	p = parseTestPresence(t, `<presence type='error'><error type='cancel' code='409'><conflict `+errNS+`/></error></presence>`)
	if !p.conflict || p.errType != "cancel" || p.errCondition != "conflict" {
		t.Fatalf("%+v", p)
	}
	if strings.Contains(fmt.Sprintf("%+v", p), "SECRET") {
		t.Fatal("text stored")
	}
	p = parseTestPresence(t, `<presence><show>away</show></presence>`)
	if p.errType != "" || p.errCondition != "" {
		t.Fatalf("%+v", p)
	}
}

func TestMUCJoinErrorPermanentAndDelay(t *testing.T) {
	for _, c := range []string{"forbidden", "registration-required", "not-authorized", "item-not-found", "not-allowed", "jid-malformed", "gone"} {
		if !mucJoinErrorPermanent(c) {
			t.Errorf("%s should be permanent", c)
		}
	}
	for _, c := range []string{"remote-server-timeout", "remote-server-not-found", "service-unavailable", "internal-server-error", "resource-constraint", "", "weird"} {
		if mucJoinErrorPermanent(c) {
			t.Errorf("%q should be transient", c)
		}
	}
	oldB, oldM := mucJoinRetryBase, mucJoinRetryMax
	defer func() { mucJoinRetryBase, mucJoinRetryMax = oldB, oldM }()
	mucJoinRetryBase, mucJoinRetryMax = 30*time.Second, 10*time.Minute
	table := map[int]time.Duration{-1: 0, 0: 0, 1: 30 * time.Second, 2: time.Minute, 3: 2 * time.Minute, 5: 8 * time.Minute, 6: 10 * time.Minute, 40: 10 * time.Minute}
	for n, want := range table {
		if got := mucJoinRetryDelay(n); got != want {
			t.Errorf("delay(%d) = %v, want %v", n, got, want)
		}
	}
}

func TestMUCJoinErrorTransientKeepsWant(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	if err := x.joinMUCNick(context.Background(), jhRoom, nil, true, ""); err != nil {
		t.Fatal(err)
	}
	w := r.want(jhRoom)
	if w == nil || w.attempts != 1 || !w.writeBookmark {
		t.Fatalf("want = %+v", w)
	}
	if !x.onMUCJoinError(jhRoom, "", "wait", "remote-server-timeout") {
		t.Fatal("not handled")
	}
	if got := r.want(jhRoom); got == nil || got.attempts != 1 {
		t.Fatalf("want after error = %+v", got)
	} else if !got.deadline.Before(w.deadline) {
		t.Fatalf("deadline not moved earlier: %v vs %v", got.deadline, w.deadline)
	}
	x.mucPendingJoinMu.Lock()
	_, pending := x.mucPendingJoin[jhRoom]
	_, track := x.mucJoinTracks[jhRoom]
	x.mucPendingJoinMu.Unlock()
	if pending || track {
		t.Fatal("pending/track not cleared")
	}
	if r.noticeCount() != 0 {
		t.Fatal("transient error posted a notice")
	}
}

func TestMUCJoinErrorPermanentDropsWant(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	_ = x.joinMUCNick(context.Background(), jhRoom, nil, false, "")
	if !x.onMUCJoinError(jhRoom, "alice", "auth", "forbidden") {
		t.Fatal("not handled")
	}
	if r.want(jhRoom) != nil {
		t.Fatal("want kept")
	}
	deadline := time.Now().Add(2 * time.Second)
	for r.noticeCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	if r.noticeCount() != 1 || !strings.Contains(r.notices[0], "forbidden") || !strings.Contains(r.notices[0], "!jabber join "+jhRoom) {
		t.Fatalf("notices = %v", r.notices)
	}
}

func TestMUCJoinErrorStaleIgnored(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	if x.onMUCJoinError(jhRoom, "", "cancel", "forbidden") {
		t.Fatal("handled without a pending join")
	}
	_ = x.joinMUCNick(context.Background(), jhRoom, nil, false, "")
	if x.onMUCJoinError(jhRoom, "someone-else", "cancel", "forbidden") {
		t.Fatal("handled for another nick")
	}
	if r.want(jhRoom) == nil {
		t.Fatal("want dropped by a stale error")
	}
	time.Sleep(30 * time.Millisecond)
	if r.noticeCount() != 0 {
		t.Fatal("notice for stale error")
	}
}

func TestMUCJoinWantLifecycle(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	ctx := context.Background()
	_ = x.joinMUCNick(ctx, jhRoom, nil, true, "")
	_ = x.joinMUCNick(ctx, jhRoom, nil, false, "held")
	w := r.want(jhRoom)
	if w == nil || w.attempts != 2 || !w.writeBookmark || w.nickOverride != "held" {
		t.Fatalf("want = %+v", w)
	}
	// Confirmation drops it.
	x.mucPendingJoinMu.Lock()
	x.noteMUCJoinConfirmedLocked(jhRoom)
	x.mucPendingJoinMu.Unlock()
	if r.want(jhRoom) != nil {
		t.Fatal("confirmed join kept")
	}
	// Leave drops it.
	x.mucLeaveHook = func(string) error { return nil }
	_ = x.joinMUCNick(ctx, jhRoom, nil, false, "")
	x.dropMUCJoinWant(jhRoom) // what leaveMUC does after the send
	if r.want(jhRoom) != nil {
		t.Fatal("left join kept")
	}
	// A failed send leaves the entry alone.
	r.x.mucSendJoinHook = func(string, string) error { return fmt.Errorf("down") }
	if err := x.joinMUCNick(ctx, jhRoom, nil, false, ""); err == nil {
		t.Fatal("expected error")
	}
	if r.want(jhRoom) != nil {
		t.Fatal("failed send created an entry")
	}
	// All nicks taken drops it.
	r.x.mucSendJoinHook = func(string, string) error { return nil }
	_ = x.joinMUCNick(ctx, jhRoom, nil, false, "")
	for i := 0; i <= mucMaxNickAlternatives; i++ {
		next, tried, gen, ok := x.onMUCJoinConflict(jhRoom, x.mucPendingJoin[jhRoom])
		if !ok {
			t.Fatal("conflict not handled")
		}
		if next == "" {
			x.continueMUCJoinAfterConflict(jhRoom, next, tried, gen)
		}
	}
	if r.want(jhRoom) != nil {
		t.Fatal("all-taken kept the entry")
	}
	// A new session clears everything.
	_ = x.joinMUCNick(ctx, jhRoom, nil, false, "")
	x.resetMUCJoinState()
	if r.want(jhRoom) != nil {
		t.Fatal("reset kept the entry")
	}
}

func TestMUCJoinWatchdogResendsUnconfirmed(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	ctx := context.Background()
	mucJoinConfirmTimeout = time.Minute
	_ = x.joinMUCNick(ctx, jhRoom, nil, false, "")
	_ = x.joinMUCNick(ctx, "ok@conference.example.com", nil, false, "")
	x.mucOurOccupantJID = map[string]string{"ok@conference.example.com": "ok@conference.example.com/alice"}
	gen := x.mucJoinGeneration()
	n0 := r.sentCount()

	x.mucWatchdogRetries(ctx, gen, time.Now()) // not due yet
	if r.sentCount() != n0 {
		t.Fatal("re-sent before the deadline")
	}
	later := time.Now().Add(70 * time.Second)
	x.mucWatchdogRetries(ctx, gen, later)
	if r.sentCount() != n0+1 {
		t.Fatalf("sent %d, want %d (confirmed room must be dropped, not re-sent)", r.sentCount(), n0+1)
	}
	if r.want("ok@conference.example.com") != nil {
		t.Fatal("confirmed room kept")
	}
	r.mu.Lock()
	h := r.hists[len(r.hists)-1]
	r.mu.Unlock()
	if h == nil || h.MaxStanzas == 0 {
		t.Fatalf("retry carried no history: %+v", h)
	}
	if w := r.want(jhRoom); w == nil || w.attempts != 2 {
		t.Fatalf("want = %+v", w)
	}
	// The deadline was pushed: an immediate second tick does not double-send.
	x.mucWatchdogRetries(ctx, gen, later)
	if r.sentCount() != n0+1 {
		t.Fatal("double send")
	}
}

func TestMUCJoinWatchdogDropsLeftRoom(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	ctx := context.Background()
	_ = x.joinMUCNick(ctx, jhRoom, nil, false, "")
	x.mucRejoinPortalsHook = func(context.Context) ([]*database.Portal, error) {
		return []*database.Portal{rejoinPortal(jhRoom, "group-dm", "!a:x", true, x.loginID)}, nil
	}
	n0 := r.sentCount()
	x.mucWatchdogRetries(ctx, x.mucJoinGeneration(), time.Now().Add(time.Hour))
	if r.sentCount() != n0 || r.want(jhRoom) != nil {
		t.Fatalf("left room: sent %d (was %d), want %+v", r.sentCount(), n0, r.want(jhRoom))
	}
}

func TestMUCJoinWatchdogStuckNoticeOnce(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	ctx := context.Background()
	gen := x.mucJoinGeneration()
	_ = x.joinMUCNick(ctx, jhRoom, nil, false, "")
	now := time.Now()
	for i := 0; i < mucJoinNoticeAfter+3; i++ {
		now = now.Add(time.Hour)
		x.mucWatchdogRetries(ctx, gen, now)
	}
	if r.noticeCount() != 1 || !strings.Contains(r.notices[0], jhRoom) {
		t.Fatalf("notices = %v", r.notices)
	}
}

func TestMUCJoinWatchdogGoroutine(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	mucWatchdogTick = 10 * time.Millisecond
	mucJoinConfirmTimeout = 20 * time.Millisecond
	mucJoinRetryBase = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { x.runMUCJoinHealth(ctx); close(done) }()
	_ = x.joinMUCNick(ctx, jhRoom, nil, false, "")
	deadline := time.Now().Add(3 * time.Second)
	for r.sentCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r.sentCount() < 2 {
		t.Fatal("no retry from the goroutine")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not stop")
	}
}

func TestMUCJoinWatchdogStopsOnNewSession(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	mucWatchdogTick = 5 * time.Millisecond
	done := make(chan struct{})
	go func() { x.runMUCJoinHealth(context.Background()); close(done) }()
	// Keep starting new sessions: the goroutine may read its generation after the first reset.
	deadline := time.After(2 * time.Second)
	for {
		x.resetMUCJoinState()
		select {
		case <-done:
			return
		case <-deadline:
			t.Fatal("watchdog survived a new session")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestClassifySelfPing(t *testing.T) {
	se := func(c stanza.Condition) error {
		return fmt.Errorf("ping: %w", stanza.Error{Type: stanza.Cancel, Condition: c})
	}
	tests := []struct {
		err  error
		want selfPingResult
	}{
		{nil, selfPingJoined},
		{se("service-unavailable"), selfPingJoined},
		{se("feature-not-implemented"), selfPingJoined},
		{se("not-acceptable"), selfPingNotJoined},
		{se("item-not-found"), selfPingJoined},
		{se("remote-server-not-found"), selfPingInconclusive},
		{se("remote-server-timeout"), selfPingInconclusive},
		{se("internal-server-error"), selfPingInconclusive},
		{context.DeadlineExceeded, selfPingInconclusive},
		{fmt.Errorf("boom"), selfPingInconclusive},
	}
	for i, tc := range tests {
		if got := classifySelfPing(tc.err); got != tc.want {
			t.Errorf("case %d (%v): got %v, want %v", i, tc.err, got, tc.want)
		}
	}
}

func TestMUCSelfPingNotJoinedRejoinsWithHeldNick(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	ctx := context.Background()
	var pinged []string
	var pingErr error
	x.pingFunc = func(_ context.Context, _ *xmpp.Session, to jid.JID) error {
		pinged = append(pinged, to.String())
		return pingErr
	}
	oldI := mucSelfPingInterval
	defer func() { mucSelfPingInterval = oldI }()
	mucSelfPingInterval = 0
	x.mucOurOccupantJID = map[string]string{jhRoom: jhRoom + "/held"}
	gen := x.mucJoinGeneration()
	at := map[string]time.Time{}
	now := time.Now()

	x.mucSelfPingTick(ctx, gen, at, now) // first sight: only recorded
	if len(pinged) != 0 {
		t.Fatalf("pinged on first sight: %v", pinged)
	}
	// service-unavailable keeps the occupant JID.
	pingErr = stanza.Error{Type: stanza.Cancel, Condition: "service-unavailable"}
	x.mucSelfPingTick(ctx, gen, at, now.Add(time.Second))
	if len(pinged) != 1 || pinged[0] != jhRoom+"/held" || x.getMUCOurOccupantJID(jhRoom) == "" || r.sentCount() != 0 {
		t.Fatalf("pinged %v, occupant %q, sent %d", pinged, x.getMUCOurOccupantJID(jhRoom), r.sentCount())
	}
	// Inconclusive keeps it too.
	pingErr = fmt.Errorf("timeout")
	x.mucSelfPingTick(ctx, gen, at, now.Add(2*time.Second))
	if x.getMUCOurOccupantJID(jhRoom) == "" || r.sentCount() != 0 {
		t.Fatal("inconclusive ping acted")
	}
	// not-acceptable forgets it and re-joins with the held nick and a history element.
	pingErr = stanza.Error{Type: stanza.Cancel, Condition: "not-acceptable"}
	x.mucSelfPingTick(ctx, gen, at, now.Add(3*time.Second))
	if x.getMUCOurOccupantJID(jhRoom) != "" {
		t.Fatal("occupant JID kept")
	}
	r.mu.Lock()
	sent := append([]string(nil), r.sent...)
	nh := len(r.hists)
	r.mu.Unlock()
	if len(sent) != 1 || sent[0] != "held" || nh != 1 {
		t.Fatalf("sent %v hists %d", sent, nh)
	}
	if w := r.want(jhRoom); w == nil || w.nickOverride != "held" {
		t.Fatalf("watchdog does not cover the re-join: %+v", w)
	}
}

func TestMUCSelfPingFirstIntervalAndCap(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	count := 0
	x.pingFunc = func(context.Context, *xmpp.Session, jid.JID) error { count++; return nil }
	oldI := mucSelfPingInterval
	defer func() { mucSelfPingInterval = oldI }()
	mucSelfPingInterval = time.Minute
	x.mucOurOccupantJID = map[string]string{}
	for i := 0; i < 8; i++ {
		room := fmt.Sprintf("r%d@conference.example.com", i)
		x.mucOurOccupantJID[room] = room + "/alice"
	}
	gen := x.mucJoinGeneration()
	at := map[string]time.Time{}
	now := time.Now()
	x.mucSelfPingTick(ctx0, gen, at, now)
	x.mucSelfPingTick(ctx0, gen, at, now.Add(30*time.Second))
	if count != 0 {
		t.Fatal("pinged before one interval")
	}
	x.mucSelfPingTick(ctx0, gen, at, now.Add(2*time.Minute))
	if count != mucSelfPingPerTick {
		t.Fatalf("pings = %d, want cap %d", count, mucSelfPingPerTick)
	}
	x.mucSelfPingTick(ctx0, gen, at, now.Add(2*time.Minute+time.Second))
	if count != 8 {
		t.Fatalf("pings = %d, want 8 after the second tick", count)
	}
}

var ctx0 = context.Background()

// A refusal or confirmation processed by Serve before joinMUCNick returns must still find the want
// entry (it is created before the send): a permanent error then leaves no entry for the watchdog.
func TestMUCJoinFastAnswerFindsWant(t *testing.T) {
	r := newJHRig(t)
	x := r.x
	x.mucSendJoinHook = func(room, nick string) error {
		x.onMUCJoinError(room, nick, "auth", "forbidden")
		return nil
	}
	_ = x.joinMUCNick(context.Background(), jhRoom, nil, false, "")
	if r.want(jhRoom) != nil {
		t.Fatal("permanent refusal before the send returned left a want entry")
	}
	// A transient error keeps the room "in flight" for the bookmark paths (leave/skip join).
	x.mucSendJoinHook = func(room, nick string) error {
		x.onMUCJoinError(room, nick, "wait", "remote-server-timeout")
		return nil
	}
	_ = x.joinMUCNick(context.Background(), jhRoom, nil, false, "")
	if r.want(jhRoom) == nil || !x.mucJoinInFlight(jhRoom) {
		t.Fatal("transient refusal: want entry missing or not in flight")
	}
}
