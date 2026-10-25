package connector

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
)

// hookClient returns a test client whose queued remote events are collected.
func hookClient() (*xmppClient, *[]bridgev2.RemoteEvent) {
	x := newOuterFromTestClient()
	var got []bridgev2.RemoteEvent
	var mu sync.Mutex // invites queue from their own goroutines
	x.queueHook = func(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, evt)
		return bridgev2.EventHandlingResultSuccess
	}
	return x, &got
}

func runHandler(t *testing.T, x *xmppClient, stanza string) {
	t.Helper()
	r, start := startOfMessage(t, stanza)
	var err error
	if typ := attrValue(start.Attr, "type"); typ == "groupchat" {
		err = x.handleGroupchatWithOOB(r, &start)
	} else {
		err = x.handleMessageWithOOB(r, &start)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// Every inbound path turns a retraction into exactly one MessageRemove event, never a message, even
// when the stanza carries a fallback body.
func TestInboundRetractionPathsQueueRemoveNotMessage(t *testing.T) {
	const fb = `<retract xmlns="urn:xmpp:message-retract:1" id="ORIG"/><body>bob retracted a message</body>`
	for _, tc := range []struct {
		name      string
		stanza    string
		portal    networkid.PortalID
		channel   string
		groupchat bool
	}{
		{"live dm", `<message type="chat" from="bob@example.com/p" id="r1">` + fb + `</message>`, "bob@example.com", retractChanLive, false},
		{"live dm without type", `<message from="bob@example.com/p" id="r1">` + fb + `</message>`, "bob@example.com", retractChanLive, false},
		{"muc self retraction", `<message type="groupchat" from="room@muc.example.com/bob" id="r1">` + fb + `</message>`, "room@muc.example.com", retractChanLive, true},
		{"muc moderation notice without body", `<message type="groupchat" from="room@muc.example.com" id="r1"><retract xmlns="urn:xmpp:message-retract:1" id="ORIG"><moderated xmlns="urn:xmpp:message-moderate:1"/></retract></message>`, "room@muc.example.com", retractChanLive, true},
		{"carbon sent", fmt.Sprintf(carbonRetractFmt, "sent", "me@example.com/laptop", "Bob@example.com"), "bob@example.com", retractChanCarbonSent, false},
		{"carbon received", fmt.Sprintf(carbonRetractFmt, "received", "Bob@example.com/p", "me@example.com"), "bob@example.com", retractChanCarbonReceived, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x, got := hookClient()
			runHandler(t, x, tc.stanza)
			if len(*got) != 1 {
				t.Fatalf("queued %d events, want 1: %v", len(*got), *got)
			}
			evt, ok := (*got)[0].(*simplevent.MessageRemove)
			if !ok {
				t.Fatalf("queued %T, want *simplevent.MessageRemove", (*got)[0])
			}
			if evt.GetPortalKey() != (networkid.PortalKey{ID: tc.portal, Receiver: retractTestLogin}) {
				t.Errorf("portal = %+v", evt.GetPortalKey())
			}
			if evt.GetTargetMessage() != "ORIG" || evt.ShouldCreatePortal() {
				t.Errorf("target=%q createPortal=%v", evt.GetTargetMessage(), evt.ShouldCreatePortal())
			}
		})
	}
}

// The MUC retraction is handled before the self-echo skip: a retraction coming from our own occupant
// (our other client) is not dropped.
func TestMUCSelfOccupantRetractionNotSkipped(t *testing.T) {
	x, got := hookClient()
	x.mucOurOccupantJID = map[string]string{"room@muc.example.com": "room@muc.example.com/me"}
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="r1"><retract xmlns="urn:xmpp:message-retract:1" id="S1"/></message>`)
	if len(*got) != 1 {
		t.Fatalf("queued %d events", len(*got))
	}
}

// Forged carbons carrying a retraction are dropped before parsing (outer from is not our account).
func TestForgedCarbonRetractionDropped(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message from="evil@example.com/x" to="me@example.com" type="chat"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/phone" to="bob@example.com" id="r1"><retract xmlns="urn:xmpp:message-retract:1" id="ORIG"/></message></forwarded></sent></message>`)
	if len(*got) != 0 {
		t.Errorf("forged retraction queued: %v", *got)
	}
}

func mamItem(id, from string, retractTarget string) mamResultItem {
	it := mamResultItem{StanzaID: id, ArchiveID: "A-" + id, From: from, To: "me@example.com", Type: "chat", Body: "text", Timestamp: time.Unix(1_700_000_000, 0)}
	if retractTarget != "" {
		it.Retract = &retractInfo{Target: retractTarget}
		it.Body = ""
	}
	return it
}

func TestMAMBatchRetractionFilter(t *testing.T) {
	items := []mamResultItem{
		mamItem("m1", "bob@example.com/p", ""),
		mamItem("m2", "bob@example.com/p", ""),
		mamItem("m3", "me@example.com/phone", ""),
		mamItem("r1", "BOB@example.com/p2", "m1"),  // bob retracts his own m1 (other resource, other case)
		mamItem("r2", "bob@example.com/p", "m3"),   // bob tries to hide OUR message: ignored
		mamItem("r3", "bob@example.com/p", "A-m2"), // by archive id
		{StanzaID: "t1", From: "bob@example.com/p", Type: "chat", Retracted: true},
	}
	b := collectBatchRetractions(items)
	want := map[string]bool{"m1": true, "m2": true, "m3": false}
	for _, it := range items[:3] {
		if got := b.hides(it); got != want[it.StanzaID] {
			t.Errorf("hides(%s) = %v, want %v", it.StanzaID, got, want[it.StanzaID])
		}
	}
	for _, it := range items {
		wantOK := it.StanzaID == "m3"
		if got := mamItemBackfillable(it, b); got != wantOK {
			t.Errorf("backfillable(%s) = %v, want %v (retractions, tombstones and hidden originals are skipped)", it.StanzaID, got, wantOK)
		}
	}
	// Collected from everything fetched: the retraction survives even if truncation drops it later.
	if !collectBatchRetractions(items[3:4]).hides(items[0]) {
		t.Error("retraction alone must hide its original")
	}
	if (batchRetractions)(nil).hides(items[0]) {
		t.Error("nil batch hides nothing")
	}
	// Moderation retractions are MUC only and not used for DM batches.
	mod := mamItem("mr", "bob@example.com/p", "m1")
	mod.Retract.Moderated = true
	if collectBatchRetractions([]mamResultItem{mod}).hides(items[0]) {
		t.Error("moderation must not hide DM originals")
	}
	// Empty-body items (tombstones, chat states) never become backfill messages.
	if mamItemBackfillable(mamResultItem{StanzaID: "x", Type: "chat"}, nil) {
		t.Error("body-less item must be skipped")
	}
}

func TestQueueMAMRetraction(t *testing.T) {
	x, got := hookClient()
	ctx := context.Background()
	pk := networkid.PortalKey{ID: "bob@example.com", Receiver: retractTestLogin}
	if x.queueMAMRetraction(ctx, pk, mamItem("m1", "bob@example.com/p", "")) || len(*got) != 0 {
		t.Fatal("a normal item is not a retraction")
	}
	if !x.queueMAMRetraction(ctx, pk, mamItem("r1", "bob@example.com/p", "m1")) || len(*got) != 1 {
		t.Fatal("retraction item must queue one event")
	}
	evt := (*got)[0].(*simplevent.MessageRemove)
	if evt.GetPortalKey() != pk || evt.GetTargetMessage() != "m1" || evt.GetTimestamp().Unix() != 1_700_000_000 {
		t.Errorf("event = %+v", evt)
	}
	// An empty target is swallowed (nothing to resolve) but is still not bridged as a message.
	empty := mamItem("r3", "bob@example.com/p", "x")
	empty.Retract.Target = ""
	if !x.queueMAMRetraction(ctx, pk, empty) || len(*got) != 1 {
		t.Error("retraction without target must be consumed without an event")
	}
}
