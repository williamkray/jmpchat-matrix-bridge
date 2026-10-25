package connector

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

const rejoinLogin networkid.UserLoginID = "alice@example.com"

func rejoinPortal(pid string, rt database.RoomType, mxid string, left bool, receiver networkid.UserLoginID) *database.Portal {
	p := &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID(pid), Receiver: receiver}, RoomType: rt}
	p.MXID = id.RoomID(mxid)
	if left {
		p.Metadata = &PortalMetadata{MUCLeft: true}
	}
	return p
}

func TestSelectMUCRejoinCandidates(t *testing.T) {
	portals := []*database.Portal{
		rejoinPortal("a@conference.example.com", database.RoomTypeGroupDM, "!a:x", false, rejoinLogin),
		rejoinPortal("nomxid@conference.example.com", database.RoomTypeGroupDM, "", false, rejoinLogin),
		rejoinPortal("left@conference.example.com", database.RoomTypeGroupDM, "!l:x", true, rejoinLogin),
		rejoinPortal("other@conference.example.com", database.RoomTypeGroupDM, "!o:x", false, "someone@else.org"),
		rejoinPortal("friend@example.com", database.RoomTypeDM, "!d:x", false, rejoinLogin),
		rejoinPortal("+15551234567@cheogram.com", database.RoomTypeDM, "!c:x", false, rejoinLogin),
		rejoinPortal("+15551234567,+15557654321@cheogram.com", database.RoomTypeGroupDM, "!g:x", false, rejoinLogin),
		rejoinPortal("legacy@conference.example.com", "", "!lg:x", false, rejoinLogin),
		rejoinPortal("legacydm@example.com", "", "!ld:x", false, rejoinLogin),
	}
	isMUC := func(_ context.Context, j string) bool { return j == "legacy@conference.example.com" }
	rooms, skipped := selectMUCRejoinCandidates(context.Background(), portals, rejoinLogin, isMUC)
	var got []string
	for _, r := range rooms {
		got = append(got, string(r.ID))
	}
	want := []string{"a@conference.example.com", "legacy@conference.example.com"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("rooms = %v, want %v", got, want)
	}
	if skipped != 5 { // left, DM, cheogram DM, group MMS, legacy DM
		t.Fatalf("skipped = %d, want 5", skipped)
	}
}

func TestMUCRejoinHistory(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	h := mucRejoinHistory(time.Time{}, now)
	if !h.Since.IsZero() || h.MaxStanzas != 20 {
		t.Fatalf("no anchor: %+v", h)
	}
	newest := now.Add(-3 * time.Hour)
	h = mucRejoinHistory(newest, now)
	if !h.Since.Equal(newest) || h.MaxStanzas != 100 {
		t.Fatalf("anchored: %+v", h)
	}
	h = mucRejoinHistory(now.Add(-30*24*time.Hour), now)
	if !h.Since.Equal(now.Add(-mucRejoinMaxAge)) {
		t.Fatalf("old anchor not clamped: %+v", h)
	}
	attrs := mucRejoinHistory(newest, now).attrs()
	if len(attrs) != 2 || attrs[0].Name.Local != "since" || attrs[0].Value != "2026-10-20T09:00:00Z" || attrs[1].Value != "100" {
		t.Fatalf("attrs = %+v", attrs)
	}
	if a := (&mucHistory{MaxStanzas: 20}).attrs(); len(a) != 1 || a[0].Name.Local != "maxstanzas" {
		t.Fatalf("fallback attrs = %+v", a)
	}
}

type rejoinRig struct {
	x     *xmppClient
	mu    sync.Mutex
	joins []string
	hist  map[string]*mucHistory
	left  map[string]bool
}

func newRejoinRig(portals []*database.Portal) *rejoinRig {
	r := &rejoinRig{hist: map[string]*mucHistory{}, left: map[string]bool{}}
	r.x = &xmppClient{loginID: rejoinLogin, log: zerolog.Nop()}
	r.x.mucSendJoinHook = func(room, nick string) error {
		r.mu.Lock()
		r.joins = append(r.joins, room)
		r.mu.Unlock()
		return nil
	}
	r.x.mucJoinHistHook = func(room string, h *mucHistory) {
		r.mu.Lock()
		r.hist[room] = h
		r.mu.Unlock()
	}
	r.x.mucLeftHook = func(room string, left bool) error {
		r.mu.Lock()
		r.left[room] = left
		r.mu.Unlock()
		return nil
	}
	r.x.mucRejoinPortalsHook = func(context.Context) ([]*database.Portal, error) { return portals, nil }
	r.x.mucNewestMsgHook = func(_ context.Context, room string) time.Time {
		if room == "a@conference.example.com" {
			return time.Now().Add(-time.Hour)
		}
		return time.Time{}
	}
	return r
}

func (r *rejoinRig) joined() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.joins...)
}

func fastRejoin(t *testing.T, interval time.Duration) {
	od, oi := mucRejoinInitialDelay, mucRejoinInterval
	mucRejoinInitialDelay, mucRejoinInterval = 0, interval
	or := bookmarkStartupRetryDelays
	bookmarkStartupRetryDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { mucRejoinInitialDelay, mucRejoinInterval, bookmarkStartupRetryDelays = od, oi, or })
}

func TestRunMUCAutoRejoin(t *testing.T) {
	fastRejoin(t, time.Millisecond)
	r := newRejoinRig([]*database.Portal{
		rejoinPortal("a@conference.example.com", database.RoomTypeGroupDM, "!a:x", false, rejoinLogin),
		rejoinPortal("b@conference.example.com", database.RoomTypeGroupDM, "!b:x", false, rejoinLogin),
		rejoinPortal("left@conference.example.com", database.RoomTypeGroupDM, "!l:x", true, rejoinLogin),
		rejoinPortal("dm@example.com", database.RoomTypeDM, "!d:x", false, rejoinLogin),
	})
	r.x.runMUCAutoRejoin(context.Background())
	if got := r.joined(); len(got) != 2 || got[0] != "a@conference.example.com" || got[1] != "b@conference.example.com" {
		t.Fatalf("joins = %v", got)
	}
	if h := r.hist["a@conference.example.com"]; h == nil || h.Since.IsZero() || h.MaxStanzas != 100 {
		t.Fatalf("history a = %+v", h)
	}
	if h := r.hist["b@conference.example.com"]; h == nil || !h.Since.IsZero() || h.MaxStanzas != 20 {
		t.Fatalf("history b = %+v", h)
	}
	// The automatic path must not touch the left flag.
	if len(r.left) != 0 {
		t.Fatalf("left flag touched: %v", r.left)
	}
}

func TestRunMUCAutoRejoinCancel(t *testing.T) {
	fastRejoin(t, 200*time.Millisecond)
	var ps []*database.Portal
	for _, n := range []string{"a", "b", "c", "d"} {
		ps = append(ps, rejoinPortal(n+"@conference.example.com", database.RoomTypeGroupDM, "!"+n+":x", false, rejoinLogin))
	}
	r := newRejoinRig(ps)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.x.runMUCAutoRejoin(ctx); close(done) }()
	deadline := time.After(2 * time.Second)
	for len(r.joined()) < 1 {
		select {
		case <-deadline:
			t.Fatal("first join never happened")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch did not stop after cancel")
	}
	if n := len(r.joined()); n >= 4 {
		t.Fatalf("joined %d rooms after cancel", n)
	}
}

func TestRunMUCAutoRejoinCancelledBeforeStart(t *testing.T) {
	od := mucRejoinInitialDelay
	mucRejoinInitialDelay = time.Hour
	defer func() { mucRejoinInitialDelay = od }()
	r := newRejoinRig([]*database.Portal{rejoinPortal("a@conference.example.com", database.RoomTypeGroupDM, "!a:x", false, rejoinLogin)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.x.runMUCAutoRejoin(ctx)
	if len(r.joined()) != 0 {
		t.Fatal("joined despite cancelled session")
	}
}

func TestJoinClearsAndLeaveSetsLeftFlag(t *testing.T) {
	r := newRejoinRig(nil)
	if err := r.x.JoinMUC(context.Background(), "a@conference.example.com"); err != nil {
		t.Fatal(err)
	}
	if left, ok := r.left["a@conference.example.com"]; !ok || left {
		t.Fatalf("join must clear the flag: %v", r.left)
	}
	// A failed leave must not set the flag; a successful one does.
	fail := true
	r.x.mucLeaveHook = func(string) error {
		if fail {
			return context.DeadlineExceeded
		}
		return nil
	}
	if err := r.x.LeaveMUC(context.Background(), "a@conference.example.com"); err == nil || r.left["a@conference.example.com"] {
		t.Fatalf("failed leave: err=%v flags=%v", err, r.left)
	}
	fail = false
	if err := r.x.LeaveMUC(context.Background(), "a@conference.example.com"); err != nil || !r.left["a@conference.example.com"] {
		t.Fatalf("leave: err=%v flags=%v", err, r.left)
	}
	// Joining again clears it.
	if err := r.x.JoinMUC(context.Background(), "a@conference.example.com"); err != nil || r.left["a@conference.example.com"] {
		t.Fatalf("rejoin: err=%v flags=%v", err, r.left)
	}
	// An explicit join sends no history element (server default).
	if h := r.hist["a@conference.example.com"]; h != nil {
		t.Fatalf("manual join history = %+v", h)
	}
}

func TestRunMUCAutoRejoinSkipsRoomLeftDuringBatch(t *testing.T) {
	fastRejoin(t, time.Millisecond)
	b := rejoinPortal("b@conference.example.com", database.RoomTypeGroupDM, "!b:x", false, rejoinLogin)
	r := newRejoinRig([]*database.Portal{
		rejoinPortal("a@conference.example.com", database.RoomTypeGroupDM, "!a:x", false, rejoinLogin),
		b,
	})
	send := r.x.mucSendJoinHook
	r.x.mucSendJoinHook = func(room, nick string) error {
		if room == "a@conference.example.com" {
			// The user leaves b while the batch is still on a.
			b.Metadata = &PortalMetadata{MUCLeft: true}
		}
		return send(room, nick)
	}
	r.x.runMUCAutoRejoin(context.Background())
	if got := r.joined(); len(got) != 1 || got[0] != "a@conference.example.com" {
		t.Fatalf("joins = %v, want only a", got)
	}
}
