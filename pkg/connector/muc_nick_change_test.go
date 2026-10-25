package connector

import (
	"context"
	"encoding/xml"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
)

// leftPortalHook lists one left MUC portal (nickRoom) of the client's login.
func leftPortalHook(x *xmppClient) func(context.Context) ([]*database.Portal, error) {
	return func(context.Context) ([]*database.Portal, error) {
		return []*database.Portal{rejoinPortal(nickRoom, database.RoomTypeGroupDM, "!room:x", true, x.loginID)}, nil
	}
}

type chRig struct {
	*nickRig
	changes   []string
	explicit  [][2]string
	bookmarks [][2]string
	left      []bool
	hist      int
}

func newChRig() *chRig {
	r := &chRig{nickRig: newNickRig()}
	x := r.x
	x.mucNickChangeHook = func(room, nick string) error {
		r.mu.Lock()
		r.changes = append(r.changes, nick)
		r.mu.Unlock()
		return nil
	}
	x.mucExplicitSaveHook = func(room, nick string) error {
		r.mu.Lock()
		r.explicit = append(r.explicit, [2]string{room, nick})
		r.mu.Unlock()
		return nil
	}
	x.joinBookmarkHook = func(room, nick string) {
		r.mu.Lock()
		r.bookmarks = append(r.bookmarks, [2]string{room, nick})
		r.mu.Unlock()
	}
	x.mucLeftHook = func(room string, left bool) error {
		r.mu.Lock()
		r.left = append(r.left, left)
		r.mu.Unlock()
		return nil
	}
	x.mucJoinHistHook = func(string, *mucHistory) {
		r.mu.Lock()
		r.hist++
		r.mu.Unlock()
	}
	return r
}

func (r *chRig) joined(nick string) {
	r.x.mucOurOccupantJIDMu.Lock()
	defer r.x.mucOurOccupantJIDMu.Unlock()
	r.x.mucOurOccupantJID = map[string]string{nickRoom: nickRoom + "/" + nick}
}

func (r *chRig) snapshot() (changes, notices, sent []string, explicit, bookmarks [][2]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.changes...), append([]string(nil), r.notices...), append([]string(nil), r.sent...),
		append([][2]string(nil), r.explicit...), append([][2]string(nil), r.bookmarks...)
}

func (r *chRig) waitBookmarks(t *testing.T, n int) [][2]string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, _, _, _, b := r.snapshot()
		if len(b) >= n {
			return b
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d bookmark writes", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const notAcceptableBody = `<error type="cancel"><not-acceptable xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error>`

func TestSanitizeMUCNick(t *testing.T) {
	long := strings.Repeat("x", 80)
	for _, tc := range []struct{ in, want, reason string }{
		{"alice", "alice", ""},
		{"a\x00b​c\x07", "abc", ""},
		{"  a \t\n b   c ", "a b c", ""},
		{long, strings.Repeat("x", mucNickMaxRunes), ""},
		{strings.Repeat("é", 70), strings.Repeat("é", mucNickMaxRunes), ""},
		{"\x07 ​", "", "empty"},
		{"", "", "empty"},
		{"bad￿nick", "", "invalid characters"},
	} {
		got, reason := sanitizeMUCNick(nickRoom, tc.in)
		if got != tc.want || reason != tc.reason {
			t.Errorf("sanitize(%q) = %q, %q; want %q, %q", tc.in, got, reason, tc.want, tc.reason)
		}
	}
	if _, reason := sanitizeMUCNick("not a jid", "alice"); reason == "" {
		t.Error("an invalid room JID must be rejected")
	}
}

func TestSetMUCNickNotJoinedSaves(t *testing.T) {
	r := newChRig()
	st, cur, err := r.x.SetMUCNick(context.Background(), nickRoom, "bob")
	if err != nil || st != MUCNickSaved || cur != "" {
		t.Fatalf("got %v %q %v", st, cur, err)
	}
	ch, _, sent, exp, _ := r.snapshot()
	if len(ch) != 0 || len(sent) != 0 || len(exp) != 1 || exp[0] != [2]string{nickRoom, "bob"} {
		t.Fatalf("changes=%v sent=%v explicit=%v", ch, sent, exp)
	}
}

func TestSetMUCNickInvalid(t *testing.T) {
	r := newChRig()
	if _, _, err := r.x.SetMUCNick(context.Background(), nickRoom, "\x07"); err == nil {
		t.Fatal("empty nick accepted")
	}
}

func TestSetMUCNickSameNick(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	st, cur, err := r.x.SetMUCNick(context.Background(), nickRoom, "alice")
	if err != nil || st != MUCNickUnchanged || cur != "alice" {
		t.Fatalf("got %v %q %v", st, cur, err)
	}
	ch, _, sent, exp, _ := r.snapshot()
	if len(ch) != 0 || len(sent) != 0 || len(exp) != 1 {
		t.Fatalf("changes=%v sent=%v explicit=%v", ch, sent, exp)
	}
}

func TestSetMUCNickLeftRoomSaves(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	// A left room (flag set in the portal) is stored, not renamed.
	r.x.mucRejoinPortalsHook = leftPortalHook(r.x)
	st, _, err := r.x.SetMUCNick(context.Background(), nickRoom, "bob")
	if err != nil || st != MUCNickSaved {
		t.Fatalf("got %v %v", st, err)
	}
	if ch, _, _, _, _ := r.snapshot(); len(ch) != 0 {
		t.Fatalf("changes = %v", ch)
	}
}

func TestSetMUCNickSendsPresenceWithoutHistory(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	var tokens int
	r.x.mucJoinTokensHook = func(string, []xml.Token) { tokens++ }
	st, cur, err := r.x.SetMUCNick(context.Background(), nickRoom, "bob")
	if err != nil || st != MUCNickChanging || cur != "alice" {
		t.Fatalf("got %v %q %v", st, cur, err)
	}
	ch, _, sent, exp, _ := r.snapshot()
	if len(ch) != 1 || ch[0] != "bob" || len(sent) != 0 || len(exp) != 0 || tokens != 0 || r.hist != 0 {
		t.Fatalf("changes=%v sent=%v explicit=%v tokens=%d hist=%d", ch, sent, exp, tokens, r.hist)
	}
}

func TestMUCNickChangeSuccess(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	if _, _, err := r.x.SetMUCNick(context.Background(), nickRoom, "bob"); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "unavailable", nickRoom+"/alice", renameBody("bob", "303", "110"))
	r.wait(t)
	if occ := r.x.getMUCOurOccupantJID(nickRoom); occ != nickRoom+"/bob" {
		t.Fatalf("occupant = %q", occ)
	}
	_, notices, _, exp, _ := r.snapshot()
	if len(exp) != 1 || exp[0][1] != "bob" || len(notices) != 1 || notices[0] != mucNickChangedOKNotice(nickRoom, "bob") {
		t.Fatalf("explicit=%v notices=%v", exp, notices)
	}
}

func TestMUCNickChangeAdjusted(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	if _, _, err := r.x.SetMUCNick(context.Background(), nickRoom, "bob"); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "unavailable", nickRoom+"/alice", renameBody("Bob2", "303", "110"))
	r.wait(t)
	_, notices, _, exp, _ := r.snapshot()
	if len(notices) != 1 || notices[0] != mucNickAdjustedNotice(nickRoom, "bob", "Bob2") || len(exp) != 1 || exp[0][1] != "bob" {
		t.Fatalf("explicit=%v notices=%v", exp, notices)
	}
	if occ := r.x.getMUCOurOccupantJID(nickRoom); occ != nickRoom+"/Bob2" {
		t.Fatalf("occupant = %q", occ)
	}
}

func TestMUCNickChangeConflictNoChain(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	if _, _, err := r.x.SetMUCNick(context.Background(), nickRoom, "bob"); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "error", nickRoom+"/bob", conflictBody)
	r.wait(t)
	r.settle()
	ch, notices, sent, exp, _ := r.snapshot()
	if len(sent) != 0 || len(exp) != 0 || len(ch) != 1 {
		t.Fatalf("sent=%v explicit=%v changes=%v", sent, exp, ch)
	}
	if len(notices) != 1 || notices[0] != mucNickTakenNotice("!jabber", nickRoom, "bob", "alice") {
		t.Fatalf("notices = %v", notices)
	}
	if occ := r.x.getMUCOurOccupantJID(nickRoom); occ != nickRoom+"/alice" {
		t.Fatalf("occupant = %q", occ)
	}
	r.x.mucPendingJoinMu.Lock()
	defer r.x.mucPendingJoinMu.Unlock()
	if len(r.x.mucJoinTracks) != 0 || len(r.x.mucPendingJoin) != 0 || len(r.x.mucNickChanges) != 0 {
		t.Fatal("join state touched by a refused nick change")
	}
}

func TestMUCNickChangeOtherError(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	if _, _, err := r.x.SetMUCNick(context.Background(), nickRoom, "bob"); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "error", nickRoom+"/bob", notAcceptableBody)
	r.wait(t)
	_, notices, _, exp, _ := r.snapshot()
	if len(notices) != 1 || notices[0] != mucNickRefusedNotice(nickRoom, "bob", "alice", "not-acceptable") || len(exp) != 0 {
		t.Fatalf("notices=%v explicit=%v", notices, exp)
	}
	r.x.mucPendingJoinMu.Lock()
	defer r.x.mucPendingJoinMu.Unlock()
	if len(r.x.mucJoinWant) != 0 {
		t.Fatal("a refused nick change created a join watchdog entry")
	}
}

func TestMUCNickChangeTimeout(t *testing.T) {
	old := mucNickChangeTimeout
	mucNickChangeTimeout = 50 * time.Millisecond
	defer func() { mucNickChangeTimeout = old }()
	r := newChRig()
	f := false
	r.x.conn = &JabberConnector{networkConfig: &JabberConfig{MUCNickSync: &f}}
	r.joined("alice")
	if _, _, err := r.x.SetMUCNick(context.Background(), nickRoom, "bob"); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	if _, n, _, _, _ := r.snapshot(); len(n) != 1 || n[0] != mucNickTimeoutNotice(nickRoom, "bob", "alice") {
		t.Fatalf("notices = %v", n)
	}
	// The late answer is handled as a foreign rename: no second notice, with sync off nothing stored.
	presenceStanza(t, r.x, "unavailable", nickRoom+"/alice", renameBody("bob", "303", "110"))
	r.settle()
	_, n, _, exp, _ := r.snapshot()
	if len(n) != 1 || len(exp) != 0 {
		t.Fatalf("notices=%v explicit=%v", n, exp)
	}
}

func TestMUCNickChangeSupersededAndReset(t *testing.T) {
	old := mucNickChangeTimeout
	defer func() { mucNickChangeTimeout = old }()
	r := newChRig()
	r.joined("alice")
	for _, n := range []string{"bob", "carol"} {
		if _, _, err := r.x.SetMUCNick(context.Background(), nickRoom, n); err != nil {
			t.Fatal(err)
		}
	}
	presenceStanza(t, r.x, "unavailable", nickRoom+"/alice", renameBody("carol", "303", "110"))
	r.wait(t)
	_, notices, _, exp, _ := r.snapshot()
	if len(notices) != 1 || !strings.Contains(notices[0], "`carol`") || len(exp) != 1 || exp[0][1] != "carol" {
		t.Fatalf("notices=%v explicit=%v", notices, exp)
	}

	// Reset stops the timer: no timeout notice afterwards.
	mucNickChangeTimeout = 50 * time.Millisecond
	r2 := newChRig()
	r2.joined("alice")
	if _, _, err := r2.x.SetMUCNick(context.Background(), nickRoom, "bob"); err != nil {
		t.Fatal(err)
	}
	r2.x.resetMUCJoinState()
	time.Sleep(150 * time.Millisecond)
	if _, n, _, _, _ := r2.snapshot(); len(n) != 0 {
		t.Fatalf("notice after reset: %v", n)
	}

	// Leaving drops it silently.
	r3 := newChRig()
	r3.joined("alice")
	r3.x.mucLeaveHook = func(string) error { return nil }
	if _, _, err := r3.x.SetMUCNick(context.Background(), nickRoom, "bob"); err != nil {
		t.Fatal(err)
	}
	if err := r3.x.leaveMUC(context.Background(), nickRoom); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, n, _, _, _ := r3.snapshot(); len(n) != 0 {
		t.Fatalf("notice after leave: %v", n)
	}
}

func TestJoinMUCStatusAlreadyInSameNick(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	st, cur, want, err := r.x.JoinMUCStatus(context.Background(), nickRoom)
	if err != nil || st != MUCJoinAlreadyIn || cur != "alice" || want != "alice" {
		t.Fatalf("got %v %q %q %v", st, cur, want, err)
	}
	b := r.waitBookmarks(t, 1)
	ch, _, sent, _, _ := r.snapshot()
	if len(sent) != 0 || len(ch) != 0 || r.hist != 0 || b[0] != [2]string{nickRoom, "alice"} {
		t.Fatalf("sent=%v changes=%v hist=%d bookmarks=%v", sent, ch, r.hist, b)
	}
	if len(r.left) != 1 || r.left[0] {
		t.Fatalf("left calls = %v", r.left)
	}
}

func TestJoinMUCStatusAlreadyInRenames(t *testing.T) {
	r := newChRig()
	r.joined("old")
	st, cur, want, err := r.x.JoinMUCStatus(context.Background(), nickRoom)
	if err != nil || st != MUCJoinRenaming || cur != "old" || want != "alice" {
		t.Fatalf("got %v %q %q %v", st, cur, want, err)
	}
	ch, _, sent, _, bm := r.snapshot()
	if len(ch) != 1 || ch[0] != "alice" || len(sent) != 0 || len(bm) != 0 {
		t.Fatalf("changes=%v sent=%v bookmarks=%v", ch, sent, bm)
	}
	presenceStanza(t, r.x, "unavailable", nickRoom+"/old", renameBody("alice", "303", "110"))
	r.wait(t) // the result notice
	if b := r.waitBookmarks(t, 1); b[0] != [2]string{nickRoom, "alice"} {
		t.Fatalf("bookmarks = %v", b)
	}

	// Refused: the bookmark is still enabled, without a nick.
	r = newChRig()
	r.joined("old")
	if _, _, _, err := r.x.JoinMUCStatus(context.Background(), nickRoom); err != nil {
		t.Fatal(err)
	}
	presenceStanza(t, r.x, "error", nickRoom+"/alice", conflictBody)
	r.wait(t)
	if b := r.waitBookmarks(t, 1); b[0] != [2]string{nickRoom, ""} {
		t.Fatalf("bookmarks = %v", b)
	}
	if _, _, sent, _, _ := r.snapshot(); len(sent) != 0 {
		t.Fatalf("join sent: %v", sent)
	}
}

func TestJoinMUCStatusNotJoinedSendsJoin(t *testing.T) {
	r := newChRig()
	st, _, _, err := r.x.JoinMUCStatus(context.Background(), nickRoom)
	if err != nil || st != MUCJoinSent {
		t.Fatalf("got %v %v", st, err)
	}
	ch, _, sent, _, _ := r.snapshot()
	if len(sent) != 1 || sent[0] != "alice" || len(ch) != 0 || r.hist != 1 {
		t.Fatalf("sent=%v changes=%v hist=%d", sent, ch, r.hist)
	}
	presenceStanza(t, r.x, "", nickRoom+"/alice", selfBody)
	if b := r.waitBookmarks(t, 1); b[0] != [2]string{nickRoom, "alice"} {
		t.Fatalf("bookmarks = %v", b)
	}
}

func TestPastOwnOccupantEchoMatched(t *testing.T) {
	r := newChRig()
	r.joined("alice")
	r.x.mucEcho.register(nickRoom, "m1")
	presenceStanza(t, r.x, "unavailable", nickRoom+"/alice", renameBody("bob", "303", "110"))
	if !r.x.handleOwnMUCEcho(nickRoom, nickRoom+"/alice", "m1", messageIDs{}) {
		t.Fatal("echo from the previous own occupant JID not recognized")
	}
	r.x.mucEcho.register(nickRoom, "m2")
	if r.x.handleOwnMUCEcho(nickRoom, nickRoom+"/carol", "m2", messageIDs{}) {
		t.Fatal("a foreign occupant's message claimed as our echo")
	}
	if !r.x.wasOwnMUCOccupant(nickRoom, nickRoom+"/alice") || r.x.wasOwnMUCOccupant(nickRoom, nickRoom+"/carol") {
		t.Fatal("past own occupant bookkeeping wrong")
	}
	// Outside the echo window the old JID no longer matches echoes.
	old := mucPastOwnEchoTTL
	defer func() { mucPastOwnEchoTTL = old }()
	if r.x.wasOwnMUCOccupantForEcho(nickRoom, nickRoom+"/alice", time.Now().Add(2*old)) {
		t.Fatal("echo window did not expire")
	}
}

func TestPastOwnOccupantNotAttributed(t *testing.T) {
	x, got := hookClient()
	x.conn = &JabberConnector{}
	x.mucOurOccupantJID = map[string]string{renameRoom: renameRoom + "/me"}
	presenceStanza(t, x, "unavailable", renameRoom+"/me", renameBody("me2", "303", "110"))
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="old1"><body>replayed or foreign</body></message>`)
	if len(*got) != 1 {
		t.Fatalf("queued %d events", len(*got))
	}
	m := (*got)[0].(*jabberRemoteMessage)
	if m.sender.IsFromMe || !m.pastOwnOccupant {
		t.Fatalf("sender=%+v pastOwnOccupant=%v", m.sender, m.pastOwnOccupant)
	}
}
