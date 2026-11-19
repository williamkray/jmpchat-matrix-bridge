//go:build sipcalls

package connector

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/williamkray/mrtc-sip-bridge/pkg/core"
	"github.com/williamkray/mrtc-sip-bridge/pkg/matrixrtc"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipaccount"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipua"
)

func TestCheogramE164(t *testing.T) {
	tests := []struct {
		jid    string
		want   string
		wantOK bool
	}{
		{"+15551234567@cheogram.com", "+15551234567", true},
		{"+15551234567@CHEOGRAM.COM", "+15551234567", true},
		{"+15551234567@cheogram.com/phone", "+15551234567", true},
		{"+15551234567,+15557654321@cheogram.com", "", false},
		{"33733;phone-context=ca-us.phone-context.soprani.ca@cheogram.com", "", false},
		{"+15551234567@example.com", "", false},
		{"15551234567@cheogram.com", "", false},
		{"+1555@cheogram.com", "", false},
		{"+1abc5551234@cheogram.com", "", false},
		{"cheogram.com", "", false},
		{"@cheogram.com", "", false},
		{"room@conference.example.com", "", false},
	}
	for _, tc := range tests {
		got, ok := cheogramE164(tc.jid)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("cheogramE164(%q) = %q, %v; want %q, %v", tc.jid, got, ok, tc.want, tc.wantOK)
		}
	}
	if j := cheogramJID("+15551234567"); j != "+15551234567@cheogram.com" {
		t.Errorf("cheogramJID = %q", j)
	}
}

func testPortal(id networkid.PortalID, receiver networkid.UserLoginID, rt database.RoomType, mxid id.RoomID) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: id, Receiver: receiver},
		MXID:      mxid,
		RoomType:  rt,
	}}
}

func TestLookupCallRoomRefusals(t *testing.T) {
	x, _ := testCallsExt(t, true)
	x.enabled = true
	const me networkid.UserLoginID = "me@example.com"
	tests := []struct {
		name   string
		portal *bridgev2.Portal
	}{
		{"no portal", nil},
		{"other receiver", testPortal("+15551234567@cheogram.com", "other@example.com", database.RoomTypeDM, "!r:x")},
		{"group dm type", testPortal("+15551234567@cheogram.com", me, database.RoomTypeGroupDM, "!r:x")},
		{"group mms", testPortal("+15551234567,+15557654321@cheogram.com", me, database.RoomTypeDM, "!r:x")},
		{"muc", testPortal("room@conference.example.com", me, database.RoomTypeGroupDM, "!r:x")},
		{"muc default type", testPortal("room@conference.example.com", me, database.RoomTypeDefault, "!r:x")},
		{"non-cheogram dm", testPortal("bob@example.com", me, database.RoomTypeDM, "!r:x")},
		{"short code", testPortal("33733;phone-context=ca-us.phone-context.soprani.ca@cheogram.com", me, database.RoomTypeDM, "!r:x")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := &jabberCallHost{x: x, lookupPortal: func(context.Context, id.RoomID) (*bridgev2.Portal, error) { return tc.portal, nil }}
			room, err := h.LookupCallRoom(context.Background(), core.LoginRef(me), "!r:x")
			if !errors.Is(err, core.ErrNotCallRoom) || room != nil {
				t.Fatalf("got %v, %v; want ErrNotCallRoom", room, err)
			}
		})
	}
	lookupErr := errors.New("db down")
	h := &jabberCallHost{x: x, lookupPortal: func(context.Context, id.RoomID) (*bridgev2.Portal, error) { return nil, lookupErr }}
	if _, err := h.LookupCallRoom(context.Background(), core.LoginRef(me), "!r:x"); !errors.Is(err, lookupErr) || errors.Is(err, core.ErrNotCallRoom) {
		t.Fatalf("lookup error: %v", err)
	}
}

func TestDecorateChatInfo(t *testing.T) {
	member, alias := matrixrtc.CallMember, matrixrtc.CallMemberAlias
	x, c := testCallsExt(t, true)
	newInfo := func() *bridgev2.ChatInfo { return &bridgev2.ChatInfo{Members: &bridgev2.ChatMemberList{}} }

	// Disabled: nothing changes.
	info := newInfo()
	x.decorateChatInfo("+15551234567@cheogram.com", info)
	if info.Members.PowerLevels != nil {
		t.Fatal("decorated while disabled")
	}

	x.enabled = true
	info = newInfo()
	x.decorateChatInfo("+15551234567@cheogram.com", info)
	if pl := info.Members.PowerLevels; pl == nil || pl.Events[member] != 0 || pl.Events[alias] != 0 ||
		pl.Events[matrixrtc.RTCSlot] != 0 || pl.Events[matrixrtc.RTCSlotAlias] != 0 || len(pl.Events) != 4 {
		t.Fatalf("1:1 not decorated: %+v", info.Members.PowerLevels)
	}

	fifty := 50
	info = newInfo()
	info.Members.PowerLevels = &bridgev2.PowerLevelOverrides{Events: map[event.Type]int{event.StateRoomName: 50, member: 100}, StateDefault: &fifty}
	x.decorateChatInfo("+15551234567@cheogram.com/res", info)
	pl := info.Members.PowerLevels
	if pl.Events[member] != 0 || pl.Events[alias] != 0 || pl.Events[matrixrtc.RTCSlot] != 0 ||
		pl.Events[event.StateRoomName] != 50 || pl.StateDefault != &fifty {
		t.Fatalf("existing overrides not merged: %+v", pl)
	}

	for _, jid := range []string{"+15551234567,+15557654321@cheogram.com", "bob@example.com", "room@conference.example.com"} {
		info = newInfo()
		x.decorateChatInfo(jid, info)
		if info.Members.PowerLevels != nil {
			t.Errorf("%s decorated", jid)
		}
	}
	x.decorateChatInfo("+15551234567@cheogram.com", &bridgev2.ChatInfo{}) // nil Members: no panic

	// Through the untagged seam: dmCreateChatResponse decorates Cheogram 1:1 chats only.
	xc := &xmppClient{loginID: "me@example.com", conn: c}
	if pl := xc.dmCreateChatResponse("+15551234567@cheogram.com").PortalInfo.Members.PowerLevels; pl == nil || pl.Events[member] != 0 {
		t.Fatalf("dmCreateChatResponse not decorated: %+v", pl)
	}
	if pl := xc.dmCreateChatResponse("+15551234567,+15557654321@cheogram.com").PortalInfo.Members.PowerLevels; pl != nil {
		t.Fatal("group MMS decorated")
	}
}

// attachFakeAccount marks loginID as having a SIP account (never started).
func attachFakeAccount(x *callsExt, loginID networkid.UserLoginID) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.accounts[loginID] = &loginCalls{id: loginID, acct: (*sipaccount.Runtime)(nil).NewAccount(sipaccount.AccountConfig{}), registered: true}
}

func TestSuppressCallNotice(t *testing.T) {
	const me networkid.UserLoginID = "me@example.com"
	x, _ := testCallsExt(t, true)
	if x.suppressCallNotice(me, "+15551234567@cheogram.com") {
		t.Fatal("suppressed while disabled")
	}
	x.enabled = true
	if x.suppressCallNotice(me, "+15551234567@cheogram.com") {
		t.Fatal("suppressed without an account")
	}
	attachFakeAccount(x, me)
	tests := []struct {
		peer string
		want bool
	}{
		{"+15551234567@cheogram.com", true},
		{"+15551234567,+15557654321@cheogram.com", false},
		{"bob@example.com", false},
		{"33733;phone-context=ca-us.phone-context.soprani.ca@cheogram.com", false},
	}
	for _, tc := range tests {
		if got := x.suppressCallNotice(me, tc.peer); got != tc.want {
			t.Errorf("suppressCallNotice(%q) = %v, want %v", tc.peer, got, tc.want)
		}
	}
	if x.suppressCallNotice("other@example.com", "+15551234567@cheogram.com") {
		t.Fatal("suppressed for a login without an account")
	}
}

// TestSuppressCallNoticeFollowsRegistration: notices are suppressed only while the account is registered.
func TestSuppressCallNoticeFollowsRegistration(t *testing.T) {
	h := newAccountsHarness(t)
	h.startRuntime(t)
	ul := testLogin(true)
	h.x.loginLoaded(ul, testClient(ul))
	waitFor(t, "UA start", func() bool { return h.ua.count() == 1 && h.x.account(ul.ID).Running() })
	const peer = "+15551234567@cheogram.com"
	acct := h.x.account(ul.ID)
	if h.x.suppressCallNotice(ul.ID, peer) {
		t.Fatal("suppressed before the account registered")
	}
	set := func(st sipua.State) {
		h.x.onState(ul.ID, acct, sipua.StateEvent{State: st})
	}
	set(sipua.StateRegistered)
	if !h.x.suppressCallNotice(ul.ID, peer) {
		t.Fatal("not suppressed while registered")
	}
	set(sipua.StateTransientError)
	if h.x.suppressCallNotice(ul.ID, peer) {
		t.Fatal("suppressed after registration was lost")
	}
	set(sipua.StateRegistered)
	set(sipua.StateAuthFailed)
	if h.x.suppressCallNotice(ul.ID, peer) {
		t.Fatal("suppressed after auth failure")
	}
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// jmiClient returns an xmppClient whose queued events fail harmlessly (the bridge DB has no tables), so
// "Queued Jingle call notice" in the log shows whether a notice was queued.
func jmiClient(t *testing.T, c *JabberConnector, logs *syncBuffer) *xmppClient {
	t.Helper()
	raw, err := dbutil.NewWithDialect("file:jmitest?mode=memory&cache=shared", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	br := c.br
	br.DB = database.New("test", c.GetDBMetaTypes(), raw)
	br.BackgroundCtx = context.Background()
	const me networkid.UserLoginID = "me@example.com"
	ul := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: me}, Bridge: br, Log: zerolog.Nop()}
	return &xmppClient{loginID: me, login: ul, conn: c, log: zerolog.New(logs)}
}

func TestJMISuppressionThroughHandler(t *testing.T) {
	x, c := testCallsExt(t, true)
	x.enabled = true
	var logs syncBuffer
	xc := jmiClient(t, c, &logs)
	const queued = "Queued Jingle call notice"
	ctx := context.Background()
	propose := func(sid string) jingleMessageInfo {
		return jingleMessageInfo{action: "propose", sid: sid, media: "audio"}
	}

	// No SIP account: Cheogram JMI still produce notices.
	xc.handleJingleMessageInitiation(ctx, propose("a1"), "+15551234567@cheogram.com", false)
	if !strings.Contains(logs.String(), queued) || xc.getCallSession("a1") == nil {
		t.Fatalf("propose without account not queued: %s", logs.String())
	}

	attachFakeAccount(x, xc.loginID)
	before := strings.Count(logs.String(), queued)
	xc.handleJingleMessageInitiation(ctx, propose("b1"), "+15551234567@cheogram.com", false)
	xc.handleJingleMessageInitiation(ctx, jingleMessageInfo{action: "retract", sid: "b1"}, "+15551234567@cheogram.com", false)
	xc.handleJingleMessageInitiation(ctx, jingleMessageInfo{action: "finish", sid: "b2"}, "+15551234567@cheogram.com", false)
	if n := strings.Count(logs.String(), queued); n != before || xc.getCallSession("b1") != nil {
		t.Fatalf("suppressed JMI queued notices (%d -> %d)", before, n)
	}

	// Real XMPP callers and group MMS JIDs are not suppressed.
	xc.handleJingleMessageInitiation(ctx, propose("c1"), "alice@example.com", false)
	xc.handleJingleMessageInitiation(ctx, jingleMessageInfo{action: "retract", sid: "c1"}, "alice@example.com", false)
	if n := strings.Count(logs.String(), queued); n != before+2 {
		t.Fatalf("plain JID JMI: %d notices, want %d", n-before, 2)
	}
	xc.handleJingleMessageInitiation(ctx, propose("d1"), "+15551234567,+15557654321@cheogram.com", false)
	if n := strings.Count(logs.String(), queued); n != before+3 {
		t.Fatal("group MMS JMI not queued")
	}
}
