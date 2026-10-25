package connector

import (
	"context"
	"testing"
)

// A bookmark that exists on the server but could not be decoded (or was cut off by the item cap) must
// never be overwritten by a freshly created item.
func TestWritesNeverCreateOverUnreadableItem(t *testing.T) {
	r := newBMRig(t, true)
	r.fetchInfo = bookmarkFetchInfo{Skipped: 1, Opaque: map[string]bool{bmRoom: true}}
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "alice", true); out != bmFailed || len(r.published()) != 0 {
		t.Fatalf("opaque item: out=%v publishes=%d", out, len(r.published()))
	}
	r = newBMRig(t, true)
	r.fetchInfo = bookmarkFetchInfo{Truncated: true}
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "alice", true); out != bmFailed || len(r.published()) != 0 {
		t.Fatalf("truncated fetch: out=%v publishes=%d", out, len(r.published()))
	}

	r = newBMRig(t, true)
	r.x.login = backfillLogin(t)
	r.fetchInfo = bookmarkFetchInfo{Skipped: 1, Opaque: map[string]bool{roomA: true}}
	res := r.x.runBookmarkBackfill(context.Background(), []string{roomA, roomF}, false)
	if res.Created != 1 || res.AlreadyHad != 1 || len(r.published()) != 1 || r.published()[0].ID != roomF {
		t.Fatalf("backfill over opaque item: %+v publishes=%+v", res, r.published())
	}
}

// The PEP echo of our own publish can arrive before the IQ result: it must still compare equal to the
// cache (no join/leave reaction), and a failed publish restores the cache.
func TestOwnPublishEchoBeforeIQResultIsIgnored(t *testing.T) {
	r := newBMRig(t, true, bm(bmRoom, false, "n"))
	r.addPortal(bmRoom, false)
	pub := r.x.bm.publishHook
	var echoActed bool
	r.x.bm.publishHook = func(ctx context.Context, c bookmarkConf) error {
		cc := c
		echoActed = r.x.applyBookmarkChange(ctx, bookmarkChange{ID: c.ID, Conf: &cc})
		return pub(ctx, c)
	}
	if out := r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true); out != bmWritten {
		t.Fatalf("out = %v", out)
	}
	if echoActed || len(r.joins()) != 0 {
		t.Fatalf("echo acted: %v joins=%v", echoActed, r.joins())
	}

	r = newBMRig(t, true, bm(bmRoom, false, "n"))
	r.publishErr = errFakeTimeout{}
	r.x.publishBookmarkFor(context.Background(), bmRoom, true, "", true)
	if c, hit, _ := r.x.bm.lookup(bmRoom); !hit || c.Autojoin {
		t.Fatalf("cache not restored after failed publish: %+v hit=%v", c, hit)
	}
}

type errFakeTimeout struct{}

func (errFakeTimeout) Error() string { return "timeout" }

// Sending/viewing in a room we are already in sends nothing (a re-join would replay the occupant list
// and history); the join command on a joined room changes only the nick.
func TestJoinForPortalKeepsCurrentNick(t *testing.T) {
	r := newBMRig(t, true)
	r.addPortal(bmRoom, false)
	r.joinedRoom(bmRoom, "renamed-elsewhere")
	if err := r.x.joinMUCForPortal(context.Background(), bmRoom); err != nil {
		t.Fatal(err)
	}
	if j := r.joins(); len(j) != 0 {
		t.Fatalf("joins = %v", j)
	}
	var changes []string
	r.x.mucNickChangeHook = func(room, nick string) error {
		changes = append(changes, room+"|"+nick)
		return nil
	}
	if err := r.x.JoinMUC(context.Background(), bmRoom); err != nil {
		t.Fatal(err)
	}
	// Already in the room: no join presence (no history replay), only a nick change.
	if j := r.joins(); len(j) != 0 {
		t.Fatalf("joins = %v", j)
	}
	if len(changes) != 1 || changes[0] != bmRoom+"|alice" {
		t.Fatalf("nick changes = %v", changes)
	}
}

// A send re-joining before the join command's join is confirmed keeps the bookmark write.
func TestJoinCommandBookmarkSurvivesInterleavedSendJoin(t *testing.T) {
	r := newBMRig(t, true)
	r.addPortal(bmRoom, false)
	if err := r.x.JoinMUC(context.Background(), bmRoom); err != nil {
		t.Fatal(err)
	}
	if err := r.x.joinMUCForPortal(context.Background(), bmRoom); err != nil {
		t.Fatal(err)
	}
	confirmJoin(t, r, bmRoom, "alice")
	bmWaitFor(t, "bookmark write", func() bool { return len(r.published()) == 1 })
}

// Occupant JIDs belong to the session: a new session starts in no room.
func TestResetMUCJoinStateForgetsOccupants(t *testing.T) {
	r := newBMRig(t, true)
	r.joinedRoom(bmRoom, "alice")
	r.x.resetMUCJoinState()
	if occ := r.x.getMUCOurOccupantJID(bmRoom); occ != "" {
		t.Fatalf("occupant survived the session reset: %q", occ)
	}
}
