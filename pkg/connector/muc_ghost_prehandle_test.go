package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// The Serve-side handler only queues; the ghost lookup runs from PreHandle in the portal loop, for
// messages and reactions of other occupants only.
func TestMUCGhostLookupRunsInPreHandleNotServe(t *testing.T) {
	x, got := hookClient()
	x.conn = &JabberConnector{}
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}
	var calls []string
	x.ensureMUCGhostHook = func(_ context.Context, occ string) { calls = append(calls, occ) }

	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="m1"><body>hi</body></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="m2"><reactions xmlns="urn:xmpp:reactions:0" id="S1"><reaction>👍</reaction></reactions></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="m3"><body>mine</body></message>`)
	if len(*got) != 3 {
		t.Fatalf("queued %d events, want 3", len(*got))
	}
	if len(calls) != 0 {
		t.Fatalf("Serve path ran the ghost lookup: %v", calls)
	}
	for _, e := range *got {
		e.(*jabberRemoteMessage).PreHandle(context.Background(), nil)
	}
	if len(calls) != 2 || calls[0] != selfEchoRoom+"/bob" || calls[1] != selfEchoRoom+"/bob" {
		t.Fatalf("PreHandle lookups = %v, want bob twice (message, reaction), none for own occupant", calls)
	}

	// DM, MAM replay and nil client never look up.
	calls = nil
	for _, m := range []*jabberRemoteMessage{
		{x: x, groupchat: false, sender: bridgev2.EventSender{Sender: networkid.UserID("a@example.com/res")}},
		{x: x, groupchat: true, backfillSource: "mam", sender: bridgev2.EventSender{Sender: networkid.UserID(selfEchoRoom + "/bob")}},
		{groupchat: true, sender: bridgev2.EventSender{Sender: networkid.UserID(selfEchoRoom + "/bob")}},
		{x: x, groupchat: true, sender: bridgev2.EventSender{Sender: networkid.UserID(selfEchoRoom)}},
	} {
		m.PreHandle(context.Background(), nil)
	}
	if len(calls) != 0 {
		t.Fatalf("unexpected lookups: %v", calls)
	}
}
