package connector

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// cdBot records the bot's replies and redactions.
type cdBot struct {
	bridgev2.MatrixAPI
	mu      sync.Mutex
	replies []string
	redact  []id.EventID
}

func (b *cdBot) SendMessage(_ context.Context, _ id.RoomID, _ event.Type, content *event.Content, _ *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch c := content.Parsed.(type) {
	case *event.MessageEventContent:
		b.replies = append(b.replies, c.Body)
	case *event.RedactionEventContent:
		b.redact = append(b.redact, c.Redacts)
	}
	return &mautrix.RespSendEvent{EventID: "$reply"}, nil
}

func (b *cdBot) all() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Join(b.replies, "\n")
}

func (b *cdBot) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.replies)
}

func (b *cdBot) last() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.replies) == 0 {
		return ""
	}
	return b.replies[len(b.replies)-1]
}

func (b *cdBot) redacted(e id.EventID) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.redact {
		if r == e {
			return true
		}
	}
	return false
}

type cdHarness struct {
	t      *testing.T
	bot    *cdBot
	user   *bridgev2.User
	ul     *bridgev2.UserLogin
	br     *bridgev2.Bridge
	client *xmppClient
	saves  int
	seq    int
}

func newCDHarness(t *testing.T, cfg CardDAVConfig, cheogram bool) *cdHarness {
	t.Helper()
	if cheogram {
		t.Setenv("JMPCHAT_BRIDGE_CHEOGRAM", "1")
	} else {
		t.Setenv("JMPCHAT_BRIDGE_CHEOGRAM", "0")
	}
	h := &cdHarness{t: t, bot: &cdBot{}}
	h.ul = cardDAVTestLogin()
	h.ul.UserMXID = "@me:example.com"
	h.user = &bridgev2.User{User: &database.User{MXID: "@me:example.com", ManagementRoom: "!mgmt:example.com"}}
	h.client = &xmppClient{loginID: h.ul.ID, login: h.ul, rosterJIDs: map[string]struct{}{}, rosterLoaded: true}
	h.ul.Client = h.client
	h.br = &bridgev2.Bridge{
		Log:     zerolog.Nop(),
		Config:  &bridgeconfig.BridgeConfig{CommandPrefix: "!jc"},
		Network: &JabberConnector{networkConfig: &JabberConfig{Cheogram: cheogram, CardDAV: cfg}},
	}
	savedLookup, savedSave := lookupCommandLogin, saveUserLogin
	lookupCommandLogin = func(*commands.Event) *bridgev2.UserLogin { return h.ul }
	saveUserLogin = func(context.Context, *bridgev2.UserLogin) error { h.saves++; return nil }
	t.Cleanup(func() { lookupCommandLogin, saveUserLogin = savedLookup, savedSave })
	return h
}

func (h *cdHarness) event(room id.RoomID, args ...string) *commands.Event {
	h.seq++
	return &commands.Event{
		Bot: h.bot, Bridge: h.br, RoomID: room, OrigRoomID: room,
		EventID: id.EventID("$evt" + string(rune('a'+h.seq))),
		User:    h.user, Args: args, RawArgs: strings.Join(args, " "),
		Ctx: context.Background(), Log: &zerolog.Logger{},
	}
}

func (h *cdHarness) run(fn func(*commands.Event), args ...string) *commands.Event {
	ce := h.event("!mgmt:example.com", args...)
	fn(ce)
	return ce
}

func (h *cdHarness) answer(msg string) *commands.Event {
	h.t.Helper()
	st := commands.LoadCommandState(h.user)
	if st == nil || st.Next == nil {
		h.t.Fatalf("no command state for answer %q", msg)
	}
	ce := h.event("!mgmt:example.com", msg)
	st.Next.Run(ce)
	return ce
}

func TestCardDAVLoginRefusals(t *testing.T) {
	h := newCDHarness(t, testCardDAVCfg(), true)
	ce := h.run(CardDAVLoginCommand, "https://alice:topsecret@dav.example.org")
	if !h.bot.redacted(ce.EventID) || !strings.Contains(h.bot.last(), "without arguments") || strings.Contains(h.bot.all(), "topsecret") {
		t.Fatalf("args must be redacted and refused: %q", h.bot.all())
	}
	// Outside the management room.
	other := h.event("!room:example.com")
	CardDAVLoginCommand(other)
	if !strings.Contains(h.bot.last(), "management room") || commands.LoadCommandState(h.user) != nil {
		t.Fatalf("room refusal: %q", h.bot.last())
	}
	// Cheogram off.
	h2 := newCDHarness(t, testCardDAVCfg(), false)
	h2.run(CardDAVLoginCommand)
	if h2.bot.last() != cardDAVNeedCheogramText {
		t.Fatalf("cheogram refusal: %q", h2.bot.last())
	}
	h2.run(CardDAVSyncCommand)
	if h2.bot.last() != cardDAVNeedCheogramText {
		t.Fatalf("sync cheogram refusal: %q", h2.bot.last())
	}
}

func TestCardDAVLoginFlowRedactsPasswordAndSaves(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	h := newCDHarness(t, testCardDAVCfg(), true)
	h.run(CardDAVLoginCommand)
	if !strings.Contains(h.bot.all(), "Please enter your CardDAV server URL") {
		t.Fatalf("no first prompt: %q", h.bot.all())
	}
	h.answer(f.srv.URL + "/dav/")
	h.answer(f.user)
	pw := h.answer(f.pass)
	if !h.bot.redacted(pw.EventID) {
		t.Fatal("password message was not redacted")
	}
	last := h.answer("skip")
	_ = last
	if commands.LoadCommandState(h.user) != nil {
		t.Fatal("command state left after the last answer")
	}
	reply := h.bot.last()
	if !strings.Contains(reply, "Connected to 127.0.0.1 as a***e: found 1 address book(s) (Contacts). Saved.") ||
		!strings.Contains(reply, "`!jc carddav-sync`") {
		t.Fatalf("reply = %q", reply)
	}
	if strings.Contains(h.bot.all(), f.pass) || strings.Contains(h.bot.all(), "/dav/alice") {
		t.Fatalf("a reply leaks the password or a path: %q", h.bot.all())
	}
	creds, ok := cardDAVCredsOf(loginMeta(h.ul))
	if !ok || creds.Password != f.pass || len(creds.Books) != 1 || h.saves != 1 {
		t.Fatalf("creds = %+v ok=%v saves=%d", creds, ok, h.saves)
	}
}

func TestCardDAVLoginRejectsURLWithUserinfo(t *testing.T) {
	h := newCDHarness(t, testCardDAVCfg(), true)
	h.run(CardDAVLoginCommand)
	ans := h.answer("https://alice:topsecret@dav.example.org/")
	if !h.bot.redacted(ans.EventID) || !strings.Contains(h.bot.last(), "Do not put a username") || strings.Contains(h.bot.all(), "topsecret") {
		t.Fatalf("userinfo URL: %q", h.bot.all())
	}
	if st := commands.LoadCommandState(h.user); st == nil {
		t.Fatal("the URL prompt should repeat")
	}
}

func TestCardDAVLoginWrongPasswordSavesNothing(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "", vc("Ann", "555 111 2222"))
	h := newCDHarness(t, testCardDAVCfg(), true)
	h.run(CardDAVLoginCommand)
	h.answer(f.srv.URL + "/dav/")
	h.answer(f.user)
	h.answer("wrong-pw-123")
	h.answer("skip")
	if !strings.Contains(h.bot.last(), "rejected the username or password") || strings.Contains(h.bot.all(), "wrong-pw-123") {
		t.Fatalf("reply = %q", h.bot.last())
	}
	if _, ok := cardDAVCredsOf(loginMeta(h.ul)); ok || h.saves != 0 {
		t.Fatal("credentials saved despite the failed login")
	}
}

func TestCardDAVLogoutAndStatus(t *testing.T) {
	h := newCDHarness(t, testCardDAVCfg(), true)
	h.run(CardDAVStatusCommand)
	if !strings.Contains(h.bot.last(), "No CardDAV account is set up for this login. Use `!jc carddav-login`.") {
		t.Fatalf("status = %q", h.bot.last())
	}
	h.run(CardDAVLogoutCommand)
	if h.bot.last() != "No CardDAV account is set up for this login." {
		t.Fatalf("logout = %q", h.bot.last())
	}
	_ = saveCardDAVCreds(context.Background(), h.ul, &cardDAVCreds{URL: "https://dav.example.org/x", Username: "alice", Password: "pw-secret", Books: []string{"https://dav.example.org/x/"}})
	h.run(CardDAVStatusCommand)
	st := h.bot.last()
	if !strings.Contains(st, "CardDAV server: dav.example.org") || !strings.Contains(st, "a***e") || strings.Contains(st, "pw-secret") || strings.Contains(st, "/x") {
		t.Fatalf("status = %q", st)
	}
	h.run(CardDAVLogoutCommand)
	if !strings.Contains(h.bot.last(), "CardDAV account removed. Contacts already added to your roster stay.") {
		t.Fatalf("logout = %q", h.bot.last())
	}
	if _, ok := cardDAVCredsOf(loginMeta(h.ul)); ok {
		t.Fatal("creds still stored")
	}
}

func waitForReplies(t *testing.T, b *cdBot, n int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for b.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for reply %d; got %q", n, b.all())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCardDAVSyncPreviewAndApplyDetached(t *testing.T) {
	f := newFakeDAV(t)
	f.addBook("Contacts", "/dav/alice/contacts/", "",
		vc("Ann", "555 111 2222"), vc("Bob", "555 111 3333"), brokenVCard,
		"BEGIN:VCARD\nVERSION:3.0\nFN:Multi\nTEL;TYPE=CELL:555 222 0001\nTEL;TYPE=WORK:555 222 0002\nEND:VCARD")
	h := newCDHarness(t, testCardDAVCfg(), true)
	h.client.rosterJIDs["+15551113333@cheogram.com"] = struct{}{} // Bob is already in the roster (named)
	h.client.roster = map[string]string{"+15551113333@cheogram.com": "Bob"}
	h.client.rosterJIDs["+15551112222@cheogram.com"] = struct{}{} // Ann is in the roster without a name
	h.run(CardDAVSyncCommand, "later")
	if !strings.Contains(h.bot.last(), "Usage:") {
		t.Fatalf("usage: %q", h.bot.last())
	}
	h.run(CardDAVSyncCommand)
	if !strings.Contains(h.bot.last(), "No CardDAV account is set up for this login. Use `!jc carddav-login`.") {
		t.Fatalf("no account: %q", h.bot.last())
	}
	if err := saveCardDAVCreds(context.Background(), h.ul, &cardDAVCreds{URL: f.srv.URL + "/dav/", Username: f.user, Password: f.pass}); err != nil {
		t.Fatal(err)
	}
	before := h.bot.count()
	ce := h.run(CardDAVSyncCommand)
	// Like withSpan, restore the handler's ctx/log after return; the detached run must not touch ce.
	ce.Ctx, ce.Log = nil, nil
	waitForReplies(t, h.bot, before+2)
	all := h.bot.all()
	if !strings.Contains(all, "CardDAV preview started. I will report back when it finishes.") {
		t.Fatalf("started text missing: %q", all)
	}
	prev := h.bot.last()
	for _, want := range []string{"CardDAV preview: 3 contacts read, 2 numbers would be added, 1 unnamed roster entry would get a name, 1 already in your roster, 1 skipped (1 bad_card).",
		"Ann: +15551112222 (other), names the existing roster entry", "Multi (mobile): +15552220001 (mobile)", "Multi (work): +15552220002 (work)", "`!jc carddav-sync apply`"} {
		if !strings.Contains(prev, want) {
			t.Errorf("preview lacks %q:\n%s", want, prev)
		}
	}
	if len(h.client.rosterJIDs) != 2 || h.client.lookupRosterName("+15551112222@cheogram.com") != "" {
		t.Fatal("preview wrote to the roster")
	}
	if strings.Contains(all, f.pass) {
		t.Fatal("password in replies")
	}
	// apply: the test client has no XMPP session, so the first add stops with a disconnect notice.
	before = h.bot.count()
	h.run(CardDAVSyncCommand, "apply")
	waitForReplies(t, h.bot, before+2)
	if !strings.Contains(h.bot.all(), "CardDAV sync started. I will report back when it finishes.") ||
		!strings.Contains(h.bot.last(), "CardDAV sync finished: 3 contacts read, 0 numbers added, 1 already in your roster, 1 skipped (1 bad_card).") ||
		!strings.Contains(h.bot.last(), "Stopped early: the Jabber connection was lost.") {
		t.Fatalf("apply reply: %q", h.bot.last())
	}
	// Status reflects the last run.
	h.run(CardDAVStatusCommand)
	if !strings.Contains(h.bot.last(), "Last sync") || !strings.Contains(h.bot.last(), "outcome disconnected") {
		t.Fatalf("status: %q", h.bot.last())
	}
}

func TestCardDAVSyncRefusals(t *testing.T) {
	h := newCDHarness(t, testCardDAVCfg(), true)
	_ = saveCardDAVCreds(context.Background(), h.ul, &cardDAVCreds{URL: "https://dav.example.org/", Username: "u", Password: "p"})
	h.client.rosterLoaded = false
	h.run(CardDAVSyncCommand)
	if h.bot.last() != "The roster has not loaded yet. Try again in a moment." {
		t.Fatalf("unloaded: %q", h.bot.last())
	}
	h.client.rosterLoaded = true
	if !beginCardDAVRun(h.ul.ID) {
		t.Fatal("setup")
	}
	defer endCardDAVRun(h.ul.ID, nil)
	h.run(CardDAVSyncCommand)
	if h.bot.last() != "A CardDAV sync is already running for this login." {
		t.Fatalf("busy: %q", h.bot.last())
	}
}

func TestMaskCardDAVUser(t *testing.T) {
	for in, want := range map[string]string{"alice": "a***e", "ab": "***", "": "***", "j.doe@example.org": "j***g"} {
		if got := maskCardDAVUser(in); got != want {
			t.Errorf("mask(%q) = %q, want %q", in, got, want)
		}
	}
}
