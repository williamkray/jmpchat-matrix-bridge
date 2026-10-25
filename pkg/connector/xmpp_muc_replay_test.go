package connector

import (
	"context"
	"errors"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// A from-me groupchat message that replays a row already bridged (Matrix-originated, or from another
// client of the user) is ignored in ConvertMessage; genuinely new messages and other occupants'
// messages are not.
func TestOwnMUCReplayDeduped(t *testing.T) {
	ctx := context.Background()
	db := realSchemaDB(t)
	const me networkid.UserLoginID = "me@example.com"
	room := networkid.PortalKey{ID: selfEchoRoom, Receiver: me}
	other := networkid.PortalKey{ID: "other@muc.example.com", Receiver: me}
	for _, p := range []networkid.PortalKey{room, other} {
		if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: p, MXID: id.RoomID("!" + string(p.ID) + ":hs")}); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	insert := func(p networkid.PortalKey, msgID string, sender networkid.UserID, meta *MessageMetadata) {
		t.Helper()
		n++
		if err := db.Message.Insert(ctx, &database.Message{
			ID: networkid.MessageID(msgID), MXID: id.EventID("$e" + string(rune('a'+n))), Room: p,
			SenderID: sender, Timestamp: time.Unix(1_700_000_000+int64(n), 0), Metadata: meta,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Sent by the bridge from Matrix (echo ids learnt, and one without a stanza-id).
	insert(room, "matrix:$sent1", "me@example.com", &MessageMetadata{XMPPMessageID: "gen1", StanzaID: "ROOM-S1"})
	insert(room, "matrix:$sent2", "me@example.com", &MessageMetadata{XMPPMessageID: "gen2"})
	// From another client of the user, bridged earlier with its origin-id.
	insert(room, "client-id", "me@example.com", &MessageMetadata{XMPPMessageID: "client-id", OriginID: "client-origin"})
	// Another occupant's message whose ids an attacker might reuse.
	insert(room, "bob-id", selfEchoRoom+"/bob", &MessageMetadata{XMPPMessageID: "bob-id", StanzaID: "ROOM-BOB", OriginID: "bob-origin"})
	// The user's message in another room.
	insert(other, "matrix:$sent3", "me@example.com", &MessageMetadata{XMPPMessageID: "gen3", StanzaID: "ROOM-S3"})

	x := newOuterFromTestClient()
	x.conn = &JabberConnector{br: &bridgev2.Bridge{DB: db}}
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: room, MXID: "!room:hs"}}

	convert := func(m *jabberRemoteMessage) bool {
		t.Helper()
		m.x, m.portalKey, m.groupchat, m.body = x, room, true, "hello"
		if m.sender.Sender == "" {
			m.sender = x.mucMessageSender(selfEchoRoom, selfEchoRoom+"/me")
		}
		_, err := m.ConvertMessage(ctx, portal, nil)
		if err != nil && !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
			t.Fatalf("ConvertMessage: %v", err)
		}
		return errors.Is(err, bridgev2.ErrIgnoringRemoteEvent)
	}
	for _, tc := range []struct {
		name    string
		m       *jabberRemoteMessage
		ignored bool
	}{
		{"bridge-sent, by room stanza-id", &jabberRemoteMessage{id: "rewritten", stanzaID: "ROOM-S1"}, true},
		{"bridge-sent, by id attr", &jabberRemoteMessage{id: "gen2"}, true},
		{"bridge-sent, by origin-id", &jabberRemoteMessage{id: "rewritten", originID: "gen1"}, true},
		{"other client, origin-id as row id", &jabberRemoteMessage{id: "rewritten", originID: "client-id"}, true},
		{"other client, origin-id alias", &jabberRemoteMessage{id: "rewritten", originID: "client-origin"}, true},
		{"new message from another client", &jabberRemoteMessage{id: "fresh", originID: "fresh-o", stanzaID: "ROOM-NEW"}, false},
		{"own ids of another occupant", &jabberRemoteMessage{id: "bob-id", originID: "bob-origin", stanzaID: "ROOM-BOB"}, false},
		{"own message of another room", &jabberRemoteMessage{id: "gen3", stanzaID: "ROOM-S3"}, false},
		{"not from me", &jabberRemoteMessage{id: "gen2", sender: x.mucMessageSender(selfEchoRoom, selfEchoRoom+"/bob")}, false},
	} {
		if got := convert(tc.m); got != tc.ignored {
			t.Errorf("%s: ignored=%v, want %v", tc.name, got, tc.ignored)
		}
	}
}

// A message from an occupant JID we held earlier (replay after a rename) gets the full own-replay check.
func TestOwnMUCReplayPastOccupant(t *testing.T) {
	ctx := context.Background()
	db := realSchemaDB(t)
	const me networkid.UserLoginID = "me@example.com"
	room := networkid.PortalKey{ID: selfEchoRoom, Receiver: me}
	if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: room, MXID: "!room:hs"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Message.Insert(ctx, &database.Message{
		ID: "matrix:$sent1", MXID: "$e1", Room: room, SenderID: "me@example.com", Timestamp: time.Unix(1_700_000_000, 0),
		Metadata: &MessageMetadata{XMPPMessageID: "gen1", OriginID: "orig1"},
	}); err != nil {
		t.Fatal(err)
	}
	x := newOuterFromTestClient()
	x.conn = &JabberConnector{br: &bridgev2.Bridge{DB: db}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: room, MXID: "!room:hs"}}
	convert := func(past bool) bool {
		m := &jabberRemoteMessage{
			x: x, portalKey: room, groupchat: true, body: "hello", id: "rewritten", originID: "orig1",
			sender: x.makeEventSender(selfEchoRoom + "/oldnick"), pastOwnOccupant: past,
		}
		_, err := m.ConvertMessage(ctx, portal, nil)
		if err != nil && !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
			t.Fatalf("ConvertMessage: %v", err)
		}
		return errors.Is(err, bridgev2.ErrIgnoringRemoteEvent)
	}
	if !convert(true) {
		t.Error("replay from a past own occupant was bridged")
	}
	if convert(false) {
		t.Error("the own-replay step ran for a foreign occupant")
	}
}
