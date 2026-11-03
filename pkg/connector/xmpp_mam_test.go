package connector

import (
	"context"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// backfillChatAsync must do nothing (and start no goroutine that touches the nil session) when
// backfill is disabled or the client has no login.
func TestBackfillChatAsyncNoOps(t *testing.T) {
	ul := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "me@example.com"}}
	for name, xc := range map[string]*xmppClient{
		"no login":          {conn: &JabberConnector{backfillConfig: &BackfillConfig{Enabled: true}}},
		"no connector":      {login: ul},
		"no backfill cfg":   {login: ul, conn: &JabberConnector{}},
		"backfill disabled": {login: ul, conn: &JabberConnector{backfillConfig: &BackfillConfig{Enabled: false}}},
	} {
		t.Run(name, func(t *testing.T) {
			xc.backfillChatAsync(context.Background(), "+15550001111@cheogram.com", "widget_chat")
			time.Sleep(20 * time.Millisecond) // a started backfill would panic on the nil session or bridge
		})
	}
}

func TestMAMEligible(t *testing.T) {
	room := map[string]struct{}{"+15550001111@cheogram.com": {}}
	known := map[string]struct{}{"+15550002222@cheogram.com": {}}
	for _, tc := range []struct {
		jid  string
		want bool
	}{
		{"alice@example.org", true},
		{"+15550001111@cheogram.com", true},                                     // number with a room
		{"+15550001111@Cheogram.com", true},                                     // case-insensitive
		{"+15550002222@cheogram.com", true},                                     // number seen on this connection
		{"+15550003333@cheogram.com", false},                                    // number without room, never seen
		{"+15550003333@cheogram.com/res", false},                                // resource ignored
		{"+15550003333,+15550004444@cheogram.com", true},                        // group MMS is not a number JID
		{"711;phone-context=ca-us.phone-context.soprani.ca@cheogram.com", true}, // short code
		{"+15550003333@other.example", true},
	} {
		if got := mamEligible(tc.jid, room, known); got != tc.want {
			t.Errorf("mamEligible(%q) = %v, want %v", tc.jid, got, tc.want)
		}
	}
	if mamEligible("+15550003333@cheogram.com", nil, nil) {
		t.Error("unknown number with nil sets must not be eligible")
	}
}

// An archived bounce from a room carries our own rejected message's body; it must never reach the
// MAM collector (it was bridged as the room itself saying our message).
func TestMAMDropsArchivedErrorBounce(t *testing.T) {
	x := newOuterFromTestClient()
	ch := x.mam.register("q1", networkid.PortalKey{ID: "room@conference.example.com", Receiver: x.loginID}, false)
	inner := `<message xmlns="jabber:client" from="room@conference.example.com" to="me@example.com/bridge" type="error" id="m1">` +
		`<body>my own words</body>` +
		`<error type="cancel"><service-unavailable xmlns="urn:ietf:params:xml:ns:xmpp-stanzas"/></error></message>`
	r, start := startOfMessage(t, `<message to="me@example.com"><result xmlns="urn:xmpp:mam:2" queryid="q1" id="A1"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"/>`+inner+`</forwarded></result></message>`)
	if err := x.handleMessageWithOOB(r, &start); err != nil {
		t.Fatal(err)
	}
	select {
	case it := <-ch:
		t.Fatalf("error bounce delivered as MAM item: %+v", it)
	default:
	}
}
