package connector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const syncUser id.UserID = "@user:example.com"

type syncRig struct {
	*chRig
	smu       sync.Mutex
	global    string
	globalErr error
	member    map[string]any
	memberErr error
	writes    []map[string]any
	dp        bool
	portal    *database.Portal
}

func newSyncRig(t *testing.T) *syncRig {
	t.Helper()
	old := nickSyncDebounce
	nickSyncDebounce = 20 * time.Millisecond
	t.Cleanup(func() { nickSyncDebounce = old })
	r := &syncRig{chRig: newChRig(), global: "Global Name", dp: true}
	x := r.x
	x.login = &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: x.loginID, UserMXID: syncUser}}
	x.queueHook = func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult { return bridgev2.EventHandlingResultSuccess }
	r.portal = rejoinPortal(nickRoom, database.RoomTypeGroupDM, "!room:x", false, x.loginID)
	x.mucRejoinPortalsHook = func(context.Context) ([]*database.Portal, error) { return []*database.Portal{r.portal}, nil }
	x.globalProfileHook = func(context.Context) (string, error) { return r.global, r.globalErr }
	x.doublePuppetHook = func() bool { return r.dp }
	x.memberStateHook = func(id.RoomID) (map[string]any, error) {
		r.smu.Lock()
		defer r.smu.Unlock()
		if r.memberErr != nil {
			return nil, r.memberErr
		}
		out := map[string]any{}
		for k, v := range r.member {
			out[k] = v
		}
		return out, nil
	}
	x.memberSetHook = func(_ id.RoomID, c map[string]any) error {
		r.smu.Lock()
		r.writes = append(r.writes, c)
		r.smu.Unlock()
		return nil
	}
	r.member = map[string]any{"membership": "join", "displayname": "Old"}
	return r
}

func (r *syncRig) setSync(on bool) {
	r.x.conn = &JabberConnector{networkConfig: &JabberConfig{MUCNickSync: &on}}
}

func (r *syncRig) change(newName, oldName string) *bridgev2.MatrixMembershipChange {
	st := string(syncUser)
	return &bridgev2.MatrixMembershipChange{
		MatrixRoomMeta: bridgev2.MatrixRoomMeta[*event.MemberEventContent]{
			MatrixEventBase: bridgev2.MatrixEventBase[*event.MemberEventContent]{
				Event:   &event.Event{Sender: syncUser, StateKey: &st, Content: event.Content{Raw: map[string]any{"displayname": newName}}},
				Content: &event.MemberEventContent{Membership: event.MembershipJoin, Displayname: newName},
				Portal:  &bridgev2.Portal{Portal: r.portal},
			},
			PrevContent: &event.MemberEventContent{Membership: event.MembershipJoin, Displayname: oldName},
		},
		Target: r.x.login,
		Type:   bridgev2.ProfileChange,
	}
}

func (r *syncRig) profile(newName, oldName string) {
	r.x.handleOwnProfileChange(context.Background(), r.change(newName, oldName))
}

func (r *syncRig) writeList() []map[string]any {
	r.smu.Lock()
	defer r.smu.Unlock()
	return append([]map[string]any(nil), r.writes...)
}

func (r *syncRig) pendingCount() int {
	r.x.nickSync.mu.Lock()
	defer r.x.nickSync.mu.Unlock()
	return len(r.x.nickSync.pending)
}

func syncWaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	bmWaitFor(t, what, cond)
}

func (r *syncRig) nothingHappened(t *testing.T) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	ch, n, sent, exp, _ := r.snapshot()
	if len(ch)+len(n)+len(sent)+len(exp)+len(r.writeList()) != 0 {
		t.Fatalf("changes=%v notices=%v sent=%v explicit=%v writes=%v", ch, n, sent, exp, r.writeList())
	}
}

func TestMucNickSyncConfigDefault(t *testing.T) {
	var c *JabberConnector
	if !c.mucNickSync() || !(&JabberConnector{}).mucNickSync() || !(&JabberConnector{networkConfig: &JabberConfig{}}).mucNickSync() {
		t.Error("default must be true")
	}
	f := false
	if (&JabberConnector{networkConfig: &JabberConfig{MUCNickSync: &f}}).mucNickSync() {
		t.Error("explicit false ignored")
	}
	example, _, upgrader := (&JabberConnector{}).GetConfig()
	if !strings.Contains(example, "\nmuc_nick_sync: true\n") {
		t.Error("example config lacks an uncommented muc_nick_sync: true")
	}
	up := func(user string) JabberConfig {
		out, err := configupgrade.DoBytes([]byte(user), &configupgrade.StructUpgrader{
			SimpleUpgrader: func(h configupgrade.Helper) { upgrader.DoUpgrade(h) },
			Base:           example,
		})
		if err != nil {
			t.Fatal(err)
		}
		var cfg JabberConfig
		if err = yaml.Unmarshal(out, &cfg); err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	if cfg := up("muc_nick_sync: false\n"); cfg.MUCNickSync == nil || *cfg.MUCNickSync {
		t.Error("user value lost by the upgrader")
	}
	if cfg := up("cheogram: true\n"); cfg.MUCNickSync == nil || !*cfg.MUCNickSync {
		t.Error("absent key must take the example value true")
	}
}

func TestProfileChangeFilters(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(r *syncRig, m *bridgev2.MatrixMembershipChange)
	}{
		{"sync off", func(r *syncRig, m *bridgev2.MatrixMembershipChange) { r.setSync(false) }},
		{"relay", func(r *syncRig, m *bridgev2.MatrixMembershipChange) { m.OrigSender = &bridgev2.OrigSender{} }},
		{"other login target", func(r *syncRig, m *bridgev2.MatrixMembershipChange) {
			m.Target = &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "other@example.com"}}
		}},
		{"sender not the user", func(r *syncRig, m *bridgev2.MatrixMembershipChange) { m.Event.Sender = "@other:example.com" }},
		{"state key not the user", func(r *syncRig, m *bridgev2.MatrixMembershipChange) {
			k := "@other:example.com"
			m.Event.StateKey = &k
		}},
		{"double puppet key", func(r *syncRig, m *bridgev2.MatrixMembershipChange) {
			m.Event.Content.Raw[appservice.DoublePuppetKey] = "x"
		}},
		{"avatar only", func(r *syncRig, m *bridgev2.MatrixMembershipChange) { m.PrevContent.Displayname = "Bob" }},
		{"empty displayname", func(r *syncRig, m *bridgev2.MatrixMembershipChange) { m.Content.Displayname = "  " }},
		{"dm portal", func(r *syncRig, m *bridgev2.MatrixMembershipChange) { r.portal.RoomType = database.RoomTypeDM }},
		{"cheogram portal", func(r *syncRig, m *bridgev2.MatrixMembershipChange) {
			r.portal.ID = "+15551234567@cheogram.com"
		}},
		{"left", func(r *syncRig, m *bridgev2.MatrixMembershipChange) {
			r.portal.Metadata = &PortalMetadata{MUCLeft: true}
		}},
		{"invite pending", func(r *syncRig, m *bridgev2.MatrixMembershipChange) {
			r.portal.Metadata = &PortalMetadata{MUCInvitePending: true}
		}},
		{"no matrix room", func(r *syncRig, m *bridgev2.MatrixMembershipChange) { r.portal.MXID = "" }},
		{"other receiver", func(r *syncRig, m *bridgev2.MatrixMembershipChange) { r.portal.Receiver = "other@example.com" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newSyncRig(t)
			m := r.change("Bob", "Old")
			tc.mut(r, m)
			r.x.handleOwnProfileChange(context.Background(), m)
			if n := r.pendingCount(); n != 0 {
				t.Fatalf("scheduled %d", n)
			}
			r.nothingHappened(t)
		})
	}
	// Control: the unmodified event is scheduled.
	r := newSyncRig(t)
	r.profile("Bob", "Old")
	if r.pendingCount() != 1 {
		t.Fatal("the plain event was not scheduled")
	}
}

func TestProfileChangeDebounced(t *testing.T) {
	r := newSyncRig(t)
	r.joined("alice")
	r.profile("Bob1", "Old")
	r.profile("Bob2", "Bob1")
	r.profile("Bob3", "Bob2")
	syncWaitFor(t, "nick change", func() bool { c, _, _, _, _ := r.snapshot(); return len(c) >= 1 })
	time.Sleep(80 * time.Millisecond)
	if ch, _, _, _, _ := r.snapshot(); len(ch) != 1 || ch[0] != "Bob3" {
		t.Fatalf("nick changes = %v", ch)
	}
	// The revert target of a burst is the display name before the first change.
	r.x.mucPendingJoinMu.Lock()
	got := r.x.mucNickChanges[nickRoom]
	r.x.mucPendingJoinMu.Unlock()
	if got == nil || got.prevDisplay != "Old" || got.display != "Bob3" {
		t.Fatalf("change = %+v", got)
	}
}

func TestProfileChangeEqualsGlobalIgnored(t *testing.T) {
	r := newSyncRig(t)
	r.joined("alice")
	r.profile("Global Name", "Bob")
	r.nothingHappened(t)
}

// A global rename makes Synapse rewrite every member event with the NEW global name. A cached (stale)
// global name must not turn that into a per-room name, or the user would be renamed in every MUC.
func TestProfileChangeStaleGlobalCacheRefetched(t *testing.T) {
	r := newSyncRig(t)
	r.joined("alice")
	if g, err := r.x.globalDisplayName(context.Background(), false); err != nil || g != "Global Name" {
		t.Fatalf("prime cache: %q %v", g, err)
	}
	r.global = "New Global" // the profile changed; the cache still holds the old name
	r.profile("New Global", "Global Name")
	r.nothingHappened(t)
}

func TestProfileChangeGlobalFetchFails(t *testing.T) {
	r := newSyncRig(t)
	r.joined("alice")
	r.globalErr = errors.New("boom")
	r.profile("Bob", "Old")
	r.nothingHappened(t)
}

func TestProfileChangeInvalidNick(t *testing.T) {
	r := newSyncRig(t)
	r.joined("alice")
	r.profile("\x07​\x01", "Old")
	syncWaitFor(t, "notice", func() bool { _, n, _, _, _ := r.snapshot(); return len(n) == 1 })
	_, n, _, _, _ := r.snapshot()
	if n[0] != mucNickSyncInvalidNotice("alice", "empty") {
		t.Fatalf("notice = %q", n[0])
	}
	if ch, _, _, _, _ := r.snapshot(); len(ch) != 0 {
		t.Fatalf("sent %v", ch)
	}
}

func TestProfileChangeNotJoinedSaves(t *testing.T) {
	r := newSyncRig(t)
	r.profile("Bob", "Old")
	syncWaitFor(t, "save", func() bool { _, _, _, e, _ := r.snapshot(); return len(e) == 1 })
	ch, n, _, e, _ := r.snapshot()
	if len(ch) != 0 || len(n) != 0 || e[0] != [2]string{nickRoom, "Bob"} {
		t.Fatalf("changes=%v notices=%v explicit=%v", ch, n, e)
	}
}

func TestProfileChangeConflictReverts(t *testing.T) {
	r := newSyncRig(t)
	r.joined("alice")
	r.member["displayname"] = "Bob"
	r.member["avatar_url"] = "mxc://example.com/a"
	r.profile("Bob", "Old")
	syncWaitFor(t, "nick change", func() bool { c, _, _, _, _ := r.snapshot(); return len(c) == 1 })
	presenceStanza(t, r.x, "error", nickRoom+"/Bob", conflictBody)
	r.wait(t)
	_, n, sent, e, _ := r.snapshot()
	if len(n) != 1 || n[0] != mucNickSyncFailedNotice("Bob", "alice", "it is taken", true) || len(sent) != 0 || len(e) != 0 {
		t.Fatalf("notices=%v sent=%v explicit=%v", n, sent, e)
	}
	w := r.writeList()
	if len(w) != 1 || w[0]["displayname"] != "Old" || w[0]["avatar_url"] != "mxc://example.com/a" {
		t.Fatalf("writes = %v", w)
	}

	// The member displayname changed meanwhile: no revert, no "set back" sentence.
	r = newSyncRig(t)
	r.joined("alice")
	r.member["displayname"] = "Else"
	r.profile("Bob", "Old")
	syncWaitFor(t, "nick change", func() bool { c, _, _, _, _ := r.snapshot(); return len(c) == 1 })
	presenceStanza(t, r.x, "error", nickRoom+"/Bob", conflictBody)
	r.wait(t)
	_, n, _, _, _ = r.snapshot()
	if len(n) != 1 || n[0] != mucNickSyncFailedNotice("Bob", "alice", "it is taken", false) || len(r.writeList()) != 0 {
		t.Fatalf("notices=%v writes=%v", n, r.writeList())
	}
}

func TestProfileChangeSuccessNoNotice(t *testing.T) {
	r := newSyncRig(t)
	r.joined("alice")
	r.member["displayname"] = "Bob"
	r.profile("Bob", "Old")
	syncWaitFor(t, "nick change", func() bool { c, _, _, _, _ := r.snapshot(); return len(c) == 1 })
	presenceStanza(t, r.x, "unavailable", nickRoom+"/alice", renameBody("Bob", "303", "110"))
	syncWaitFor(t, "save", func() bool { _, _, _, e, _ := r.snapshot(); return len(e) == 1 })
	time.Sleep(60 * time.Millisecond)
	_, n, _, e, _ := r.snapshot()
	if len(n) != 0 || e[0][1] != "Bob" || len(r.writeList()) != 0 {
		t.Fatalf("notices=%v explicit=%v writes=%v", n, e, r.writeList())
	}
}

func TestOwnNickToMatrixWrites(t *testing.T) {
	foreign := func(r *syncRig) {
		r.joined("alice")
		presenceStanza(t, r.x, "unavailable", nickRoom+"/alice", renameBody("bob", "303", "110"))
	}
	r := newSyncRig(t)
	r.member["avatar_url"] = "mxc://example.com/a"
	r.member["custom.key"] = "v"
	r.member["reason"] = "because"
	foreign(r)
	syncWaitFor(t, "write", func() bool { return len(r.writeList()) == 1 })
	_, _, _, e, _ := r.snapshot()
	w := r.writeList()[0]
	if len(e) != 1 || e[0][1] != "bob" || w["displayname"] != "bob" || w["avatar_url"] != "mxc://example.com/a" || w["custom.key"] != "v" || w["membership"] != "join" {
		t.Fatalf("explicit=%v write=%v", e, w)
	}
	if _, ok := w["reason"]; ok {
		t.Fatal("reason kept")
	}

	// Skips.
	for name, setup := range map[string]func(r *syncRig){
		"already equal": func(r *syncRig) { r.member["displayname"] = "bob" },
		"not joined":    func(r *syncRig) { r.member["membership"] = "leave" },
		"no puppet":     func(r *syncRig) { r.dp = false },
	} {
		r := newSyncRig(t)
		setup(r)
		foreign(r)
		syncWaitFor(t, name+": save", func() bool { _, _, _, e, _ := r.snapshot(); return len(e) == 1 })
		time.Sleep(60 * time.Millisecond)
		if len(r.writeList()) != 0 {
			t.Fatalf("%s: wrote %v", name, r.writeList())
		}
	}
	// Sync off: the foreign rename is followed in memory only.
	r = newSyncRig(t)
	r.setSync(false)
	foreign(r)
	r.nothingHappened(t)
	if occ := r.x.getMUCOurOccupantJID(nickRoom); occ != nickRoom+"/bob" {
		t.Fatalf("occupant = %q", occ)
	}
}

func TestOwnNickToMatrixOnAlternativeAndServerAssigned(t *testing.T) {
	// 409 alternative.
	r := newSyncRig(t)
	if err := r.x.joinMUC(context.Background(), nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "error", nickRoom+"/alice", conflictBody)
	r.wait(t) // the retry join
	presenceStanza(t, r.x, "", nickRoom+"/alice_1", selfBody)
	syncWaitFor(t, "write", func() bool { return len(r.writeList()) == 1 })
	if w := r.writeList()[0]; w["displayname"] != "alice_1" {
		t.Fatalf("write = %v", w)
	}

	// Server-assigned nick (status 110 under another nick).
	r = newSyncRig(t)
	if err := r.x.joinMUC(context.Background(), nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "", nickRoom+"/assigned", `<x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/><status code="210"/></x>`)
	syncWaitFor(t, "write", func() bool { return len(r.writeList()) == 1 })
	if w := r.writeList()[0]; w["displayname"] != "assigned" {
		t.Fatalf("write = %v", w)
	}

	// Plain join under the chosen nick: nothing.
	r = newSyncRig(t)
	if err := r.x.joinMUC(context.Background(), nickRoom, nil); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "", nickRoom+"/alice", selfBody)
	time.Sleep(100 * time.Millisecond)
	if len(r.writeList()) != 0 {
		t.Fatalf("plain join wrote %v", r.writeList())
	}
}

func TestProfileChangeReturnsNilWithSyncOffAndOn(t *testing.T) {
	for _, on := range []bool{true, false} {
		r := newSyncRig(t)
		r.setSync(on)
		res, err := r.x.HandleMatrixMembership(context.Background(), r.change("Bob", "Old"))
		if res != nil || err != nil {
			t.Fatalf("sync=%v: %v %v", on, res, err)
		}
	}
}

func TestNickSyncDoesNotLogDisplayNames(t *testing.T) {
	const secret = "SENTINELNAME"
	r := newSyncRig(t)
	buf := &bmLogBuf{}
	r.x.log = zerolog.New(buf).Level(zerolog.TraceLevel)
	r.joined("alice")
	// Valid name, joined room: a nick change is sent.
	r.profile(secret+" valid", "Old")
	syncWaitFor(t, "nick change", func() bool { c, _, _, _, _ := r.snapshot(); return len(c) == 1 })
	// Invalid name.
	r.profile(secret+"￿", secret+" valid")
	syncWaitFor(t, "notice", func() bool { _, n, _, _, _ := r.snapshot(); return len(n) == 1 })
	// Not joined: stored.
	r2 := newSyncRig(t)
	r2.x.log = r.x.log
	r2.profile(secret+" stored", "Old")
	syncWaitFor(t, "save", func() bool { _, _, _, e, _ := r2.snapshot(); return len(e) == 1 })
	// A refused change (reverts).
	presenceStanza(t, r.x, "error", nickRoom+"/"+secret+" valid", conflictBody)
	r.wait(t)
	if out := buf.String(); out == "" {
		t.Fatal("nothing logged; the test proves nothing")
	} else if strings.Contains(out, secret) {
		t.Fatalf("display name leaked into the logs:\n%s", out)
	}
}
