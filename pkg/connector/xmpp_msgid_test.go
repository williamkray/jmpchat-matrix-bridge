package connector

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// msgHookClient is hookClient with a connector that has no backfill config (nothing is triggered).
func msgHookClient() (*xmppClient, *[]bridgev2.RemoteEvent) {
	x, got := hookClient()
	x.conn = &JabberConnector{}
	return x, got
}

// onlyMessage returns the single queued jabberRemoteMessage.
func onlyMessage(t *testing.T, got *[]bridgev2.RemoteEvent) *jabberRemoteMessage {
	t.Helper()
	if len(*got) != 1 {
		t.Fatalf("queued %d events, want 1", len(*got))
	}
	m, ok := (*got)[0].(*jabberRemoteMessage)
	if !ok {
		t.Fatalf("queued %T, want *jabberRemoteMessage", (*got)[0])
	}
	return m
}

func TestInboundMessageIDOrder(t *testing.T) {
	stamp := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	full := inboundIDs{idAttr: "A", originID: "O", stanzaID: "S", scope: "me@example.com", delay: stamp}
	if got := inboundMessageID(full); got != "A" {
		t.Errorf("id attr must win, got %q", got)
	}
	full.idAttr = ""
	if got := inboundMessageID(full); got != "xmp:sid:me@example.com:S" {
		t.Errorf("trusted stanza-id next (before the sender-chosen origin-id), got %q", got)
	}
	noSID := full
	noSID.stanzaID = ""
	if got := inboundMessageID(noSID); got != "O" {
		t.Errorf("origin-id (raw) after the stanza-id, got %q", got)
	}
	full.originID = ""
	upper := full
	upper.scope = "Me@Example.com"
	if inboundMessageID(upper) != inboundMessageID(full) {
		t.Error("scope must be case-insensitive")
	}
	other := full
	other.scope = "room@muc.example.com"
	if inboundMessageID(other) == inboundMessageID(full) {
		t.Error("a different scope must give a different id")
	}

	hashed := inboundIDs{scope: "room@muc.example.com", delay: stamp, senderFull: "room@muc.example.com/bob", body: "hi", oob: ""}
	h := inboundMessageID(hashed)
	if !regexp.MustCompile(`^xmp:h:[0-9a-f]{32}$`).MatchString(string(h)) {
		t.Fatalf("hash id = %q", h)
	}
	if inboundMessageID(hashed) != h {
		t.Error("hash id must be deterministic")
	}
	for name, mut := range map[string]func(*inboundIDs){
		"body":   func(i *inboundIDs) { i.body = "ho" },
		"oob":    func(i *inboundIDs) { i.oob = "https://example.com/x" },
		"sender": func(i *inboundIDs) { i.senderFull = "room@muc.example.com/eve" },
		"stamp":  func(i *inboundIDs) { i.delay = stamp.Add(time.Second) },
		"scope":  func(i *inboundIDs) { i.scope = "other@muc.example.com" },
	} {
		v := hashed
		mut(&v)
		if inboundMessageID(v) == h {
			t.Errorf("hash id must change with %s", name)
		}
	}

	a, b := inboundMessageID(inboundIDs{}), inboundMessageID(inboundIDs{})
	if a == b || !strings.HasPrefix(string(a), randIDPrefix) || !strings.HasPrefix(string(b), randIDPrefix) {
		t.Errorf("random ids = %q, %q", a, b)
	}
}

func TestTrustedDelay(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	d := func(from string, st time.Time) delayRef { return delayRef{From: from, Stamp: st} }
	early, late := now.Add(-48*time.Hour), now.Add(-24*time.Hour)
	muc := mucDelayTrust("room@muc.example.com")
	dm := dmDelayTrust("me@example.com", "bob@transport.example.net")

	for _, tc := range []struct {
		name  string
		delay []delayRef
		trust func(string) bool
		want  time.Time
	}{
		{"muc room", []delayRef{d("room@muc.example.com", early)}, muc, early},
		{"muc room/nick", []delayRef{d("room@muc.example.com/bob", early)}, muc, early},
		{"muc other from", []delayRef{d("evil@example.com", early)}, muc, time.Time{}},
		{"muc empty from", []delayRef{d("", early)}, muc, time.Time{}},
		{"dm empty from", []delayRef{d("", early)}, dm, early},
		{"dm own domain", []delayRef{d("example.com", early)}, dm, early},
		{"dm sender domain", []delayRef{d("transport.example.net", early)}, dm, early},
		{"dm third party", []delayRef{d("evil.example.org", early)}, dm, time.Time{}},
		{"earliest wins", []delayRef{d("", late), d("", early)}, dm, early},
		{"untrusted earlier ignored", []delayRef{d("evil.example.org", early), d("", late)}, dm, late},
		{"future clamped", []delayRef{d("", now.Add(time.Hour))}, dm, now},
		{"pre-2000 ignored", []delayRef{d("", time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC))}, dm, time.Time{}},
		{"none", nil, dm, time.Time{}},
	} {
		got := trustedDelay(messageExtras{Delays: tc.delay}, now, tc.trust)
		if !got.Equal(tc.want) {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
}

func TestExtrasDelayRefs(t *testing.T) {
	ids := parseIDs(t, `<message from="a@example.com"><delay xmlns="urn:xmpp:delay" from="room@muc.example.com" stamp="2026-01-02T03:04:05.678Z"/></message>`)
	if !ids.Extras.Delayed || len(ids.Extras.Delays) != 1 {
		t.Fatalf("extras = %+v", ids.Extras)
	}
	got := ids.Extras.Delays[0]
	if got.From != "room@muc.example.com" || !got.Stamp.Equal(time.Date(2026, 1, 2, 3, 4, 5, 678_000_000, time.UTC)) {
		t.Errorf("delay = %+v", got)
	}

	// A delay below a direct child is not a direct-child delay.
	ids = parseIDs(t, `<message from="a@example.com"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-02T03:04:05Z"/></forwarded></message>`)
	if ids.Extras.Delayed || len(ids.Extras.Delays) != 0 {
		t.Errorf("nested delay recorded: %+v", ids.Extras)
	}

	var b strings.Builder
	b.WriteString(`<message from="a@example.com">`)
	for i := 0; i < 10; i++ {
		b.WriteString(`<delay xmlns="urn:xmpp:delay" stamp="2026-01-02T03:04:05Z"/>`)
	}
	b.WriteString(`</message>`)
	if ids = parseIDs(t, b.String()); len(ids.Extras.Delays) != maxDelayRefs {
		t.Errorf("delays = %d, want cap %d", len(ids.Extras.Delays), maxDelayRefs)
	}

	ids = parseIDs(t, `<message from="a@example.com"><delay xmlns="urn:xmpp:delay" stamp="garbage"/></message>`)
	if !ids.Extras.Delayed || len(ids.Extras.Delays) != 0 {
		t.Errorf("unparsable stamp: %+v", ids.Extras)
	}
}

func TestRemoteMessageGetTimestamp(t *testing.T) {
	var _ bridgev2.RemoteEventWithTimestamp = (*jabberRemoteMessage)(nil)
	ts := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	if got := (&jabberRemoteMessage{timestamp: ts}).GetTimestamp(); !got.Equal(ts) {
		t.Errorf("got %v", got)
	}
	if got := (&jabberRemoteMessage{}).GetTimestamp(); time.Since(got) > time.Second || time.Since(got) < -time.Second {
		t.Errorf("zero timestamp gave %v", got)
	}
}

func TestLiveIDLessDMUsesArchiveID(t *testing.T) {
	const tmpl = `<message type="chat" from="bob@example.com/p"><body>hi</body>%s</message>`
	x, got := msgHookClient()
	runHandler(t, x, fmt.Sprintf(tmpl, `<stanza-id xmlns="urn:xmpp:sid:0" by="me@example.com" id="ARCH1"/>`))
	if m := onlyMessage(t, got); m.id != "xmp:sid:me@example.com:ARCH1" || m.idAttr != "" || m.stanzaID != "ARCH1" {
		t.Errorf("id = %q idAttr = %q stanza = %q", m.id, m.idAttr, m.stanzaID)
	}

	x, got = msgHookClient()
	runHandler(t, x, fmt.Sprintf(tmpl, `<stanza-id xmlns="urn:xmpp:sid:0" by="evil@example.com" id="ARCH1"/>`))
	if m := onlyMessage(t, got); !strings.HasPrefix(string(m.id), randIDPrefix) {
		t.Errorf("untrusted stanza-id used: %q", m.id)
	}

	x, got = msgHookClient()
	runHandler(t, x, fmt.Sprintf(tmpl, `<origin-id xmlns="urn:xmpp:sid:0" id="ORIG1"/>`))
	if m := onlyMessage(t, got); m.id != "ORIG1" {
		t.Errorf("origin-id not used: %q", m.id)
	}

	// The MAM copy of the first message gets the same row id.
	item := mamResultItem{StanzaID: "", ArchiveID: "ARCH1"}
	if id := x.mamItemMessageID(item); id != "xmp:sid:me@example.com:ARCH1" {
		t.Errorf("mam id = %q", id)
	}
}

func TestLiveGroupchatDelay(t *testing.T) {
	const room = "room@muc.example.com"
	stamp := "2026-01-01T00:00:00Z"
	want, _ := time.Parse(time.RFC3339, stamp)
	hist := func(body, extra string) string {
		return `<message type="groupchat" from="` + room + `/bob"><body>` + body + `</body>` + extra + `</message>`
	}
	trusted := `<delay xmlns="urn:xmpp:delay" from="` + room + `" stamp="` + stamp + `"/>`

	x, got := msgHookClient()
	runHandler(t, x, hist("hi", trusted))
	if m := onlyMessage(t, got); !m.timestamp.Equal(want) || !m.GetTimestamp().Equal(want) {
		t.Errorf("timestamp = %v, want %v", m.timestamp, want)
	}

	x, got = msgHookClient()
	runHandler(t, x, hist("hi", `<delay xmlns="urn:xmpp:delay" from="evil@example.com" stamp="`+stamp+`"/>`))
	if m := onlyMessage(t, got); time.Since(m.timestamp) > time.Minute {
		t.Errorf("untrusted delay used: %v", m.timestamp)
	}

	x, got = msgHookClient()
	runHandler(t, x, hist("hi", `<stanza-id xmlns="urn:xmpp:sid:0" by="`+room+`" id="S1"/>`))
	if m := onlyMessage(t, got); m.id != "xmp:sid:room@muc.example.com:S1" {
		t.Errorf("id = %q", m.id)
	}

	ids := map[string]networkid.MessageID{}
	for name, body := range map[string]string{"first": "hi", "replay": "hi", "other": "ho"} {
		x, got = msgHookClient()
		runHandler(t, x, hist(body, trusted))
		ids[name] = onlyMessage(t, got).id
	}
	if !strings.HasPrefix(string(ids["first"]), hashIDPrefix) || ids["first"] != ids["replay"] || ids["first"] == ids["other"] {
		t.Errorf("hash ids = %v", ids)
	}
}

func TestOfflineDMDelay(t *testing.T) {
	const tmpl = `<message type="chat" from="bob@example.com/p" id="m1"><body>hi</body><delay xmlns="urn:xmpp:delay" from="example.com" stamp="%s"/></message>`
	x, got := msgHookClient()
	runHandler(t, x, fmt.Sprintf(tmpl, "2026-01-01T00:00:00Z"))
	want, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z")
	if m := onlyMessage(t, got); !m.timestamp.Equal(want) {
		t.Errorf("timestamp = %v", m.timestamp)
	}
	x, got = msgHookClient()
	runHandler(t, x, fmt.Sprintf(tmpl, time.Now().Add(48*time.Hour).UTC().Format(time.RFC3339)))
	if m := onlyMessage(t, got); m.timestamp.After(time.Now().Add(time.Second)) {
		t.Errorf("future stamp not clamped: %v", m.timestamp)
	}
}

func TestBridgedDuplicate(t *testing.T) {
	ctx := context.Background()
	db := realSchemaDB(t)
	const me networkid.UserLoginID = "me@example.com"
	pa := networkid.PortalKey{ID: "room@muc.example.com", Receiver: me}
	pb := networkid.PortalKey{ID: "bob@example.com", Receiver: me}
	for _, p := range []networkid.PortalKey{pa, pb} {
		if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: p, MXID: id.RoomID("!" + string(p.ID) + ":hs")}); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	insert := func(p networkid.PortalKey, msgID string, sender networkid.UserID, age time.Duration, meta *MessageMetadata) {
		t.Helper()
		n++
		if err := db.Message.Insert(ctx, &database.Message{
			ID: networkid.MessageID(msgID), MXID: id.EventID(fmt.Sprintf("$e%d", n)), Room: p,
			SenderID: sender, Timestamp: time.Now().Add(-age), Metadata: meta,
		}); err != nil {
			t.Fatal(err)
		}
	}
	const alice networkid.UserID = "room@muc.example.com/alice"
	insert(pa, "row-sid", alice, time.Hour, &MessageMetadata{XMPPMessageID: "row-sid", StanzaID: "S1", OccupantID: "OCC1"})
	insert(pa, "row-orig", alice, time.Hour, &MessageMetadata{XMPPMessageID: "row-orig", OriginID: "O1"})
	insert(pa, "O2", alice, time.Hour, &MessageMetadata{XMPPMessageID: "O2"})
	insert(pa, "matrix:$m", "me@example.com", time.Hour, &MessageMetadata{XMPPMessageID: "gen-1"})
	insert(pa, "row-old", alice, 40*24*time.Hour, &MessageMetadata{XMPPMessageID: "row-old", StanzaID: "SOLD"})
	insert(pb, "row-b", "bob@example.com", time.Hour, &MessageMetadata{XMPPMessageID: "row-b", StanzaID: "SB"})

	base := dupCandidate{id: "new", sender: alice, self: "me@example.com", ts: time.Now()}
	with := func(f func(*dupCandidate)) dupCandidate { c := base; f(&c); return c }
	for _, tc := range []struct {
		name   string
		portal networkid.PortalKey
		c      dupCandidate
		want   networkid.MessageID
	}{
		{"stanza-id same sender", pa, with(func(c *dupCandidate) { c.stanzaID = "S1" }), "row-sid"},
		{"stanza-id other sender", pa, with(func(c *dupCandidate) { c.stanzaID = "S1"; c.sender = "room@muc.example.com/eve" }), ""},
		{"contact origin-id alias is kept (sender-chosen)", pa, with(func(c *dupCandidate) { c.originID = "O1" }), ""},
		{"contact origin-id as row id is kept (sender-chosen)", pa, with(func(c *dupCandidate) { c.originID = "O2" }), ""},
		{"from me vs matrix row by id attr", pa, with(func(c *dupCandidate) {
			c.idAttr = "gen-1"
			c.sender = "me@example.com"
			c.fromMe = true
		}), "matrix:$m"},
		{"from me with resource sender", pa, with(func(c *dupCandidate) {
			c.idAttr = "gen-1"
			c.sender = "me@example.com/phone"
			c.fromMe = true
		}), "matrix:$m"},
		{"not from me vs matrix row", pa, with(func(c *dupCandidate) { c.idAttr = "gen-1" }), ""},
		{"occupant-id with a different nick", pa, with(func(c *dupCandidate) {
			c.stanzaID = "S1"
			c.sender = "room@muc.example.com/alice2"
			c.occupantID = "OCC1"
		}), "row-sid"},
		{"other portal", pb, with(func(c *dupCandidate) { c.stanzaID = "S1" }), ""},
		{"own id never counts", pa, with(func(c *dupCandidate) { c.id = "row-sid"; c.stanzaID = "S1" }), ""},
		{"older than window", pa, with(func(c *dupCandidate) { c.stanzaID = "SOLD" }), ""},
		// A sender that reuses origin-ids (constant, restarted counter, one SMS split into several
		// stanzas) must not lose a distinct message that has its own id attribute.
		{"contact with id attr, reused origin-id", pa, with(func(c *dupCandidate) { c.id, c.idAttr, c.originID = "A2", "A2", "O1" }), ""},
		{"contact with id attr, origin-id equals a row id", pa, with(func(c *dupCandidate) { c.id, c.idAttr, c.originID = "A3", "A3", "O2" }), ""},
		{"contact id attr equals another row's origin-id", pa, with(func(c *dupCandidate) { c.id, c.idAttr = "O1", "O1" }), ""},
		{"contact with id attr, trusted stanza-id still matches", pa, with(func(c *dupCandidate) { c.id, c.idAttr, c.stanzaID = "A4", "A4", "S1" }), "row-sid"},
		{"from me with id attr, origin-id vs matrix row", pa, with(func(c *dupCandidate) {
			c.id, c.idAttr, c.originID = "gen-1", "gen-1", "gen-1"
			c.sender = "me@example.com"
			c.fromMe = true
		}), "matrix:$m"},
	} {
		got, err := bridgedDuplicate(ctx, db, tc.portal, tc.c)
		if err != nil || got != tc.want {
			t.Errorf("%s: got %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
}

func TestConvertMessageDropsCrossPathCopy(t *testing.T) {
	ctx := context.Background()
	db := realSchemaDB(t)
	const me networkid.UserLoginID = "me@example.com"
	key := networkid.PortalKey{ID: "bob@example.com", Receiver: me}
	if err := db.Portal.Insert(ctx, &database.Portal{PortalKey: key, MXID: "!bob:hs"}); err != nil {
		t.Fatal(err)
	}
	// A row bridged before the upgrade: fabricated id, the archive id stored in metadata.
	if err := db.Message.Insert(ctx, &database.Message{
		ID: "xmp:20261024120000.000", MXID: "$legacy", Room: key, SenderID: "bob@example.com",
		Timestamp: time.Now().Add(-time.Minute), Metadata: &MessageMetadata{XMPPMessageID: "xmp:20261024120000.000", StanzaID: "ARCH1"},
	}); err != nil {
		t.Fatal(err)
	}
	x := newOuterFromTestClient()
	x.conn = &JabberConnector{br: &bridgev2.Bridge{DB: db}}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: key, MXID: "!bob:hs"}}
	convert := func(m *jabberRemoteMessage) error {
		m.x, m.portalKey, m.body, m.sender = x, key, "hello", x.makeEventSender("bob@example.com")
		_, err := m.ConvertMessage(ctx, portal, nil)
		return err
	}
	dup := &jabberRemoteMessage{id: "xmp:sid:me@example.com:ARCH1", stanzaID: "ARCH1", timestamp: time.Now()}
	if err := convert(dup); !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("cross-path copy: err = %v, want ErrIgnoringRemoteEvent", err)
	}
	if err := convert(&jabberRemoteMessage{id: "xmp:sid:me@example.com:ARCH2", stanzaID: "ARCH2", timestamp: time.Now()}); err != nil {
		t.Errorf("unrelated message: err = %v", err)
	}
}

func TestMAMItemNoFabricatedID(t *testing.T) {
	x := newOuterFromTestClient()
	parse := func(archive string) mamResultItem {
		t.Helper()
		ch := x.mam.register("q1", networkid.PortalKey{ID: "bob@example.com", Receiver: x.loginID}, false)
		r, start := startOfMessage(t, `<message to="me@example.com"><result xmlns="urn:xmpp:mam:2" queryid="q1" id="`+archive+`"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"/><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat"><body>hello</body></message></forwarded></result></message>`)
		if err := x.handleMessageWithOOB(r, &start); err != nil {
			t.Fatal(err)
		}
		item := <-ch
		x.mam.closeAndRemove("q1")
		return item
	}
	a1, a2, b := parse("A1"), parse("A1"), parse("B1")
	if a1.StanzaID != "" {
		t.Errorf("StanzaID = %q, want empty for an id-less message", a1.StanzaID)
	}
	if x.mamItemMessageID(a1) != x.mamItemMessageID(a2) {
		t.Error("parsing the same result twice must give the same id")
	}
	if x.mamItemMessageID(a1) == x.mamItemMessageID(b) {
		t.Error("different archive ids must give different ids")
	}
}

func TestFoldedCorrectionKeyIDLessOriginal(t *testing.T) {
	orig := corrItem("", "bob@example.com/p", "", "first", 1)
	orig.ArchiveID = "A1"
	f := foldedCorrection{orig: orig, corr: corrItem("c1", "bob@example.com/p", "", "second", 2)}
	keys := foldedCorrectionKeys(retractTestLogin, "bob@example.com", []foldedCorrection{f})
	want := "xmp:sid:" + strings.ToLower(bareJID(string(retractTestLogin))) + ":A1"
	if len(keys) == 0 {
		t.Fatal("no keys")
	}
	for _, v := range keys {
		if v != want {
			t.Errorf("key value = %q, want %q", v, want)
		}
	}
	x := newOuterFromTestClient()
	x.loginID = retractTestLogin
	if got := x.mamItemMessageID(orig); string(got) != want {
		t.Errorf("FetchMessages id = %q, want %q", got, want)
	}
}

// TestIDLessDMDelayNoHash: two distinct id-less 1:1 messages with the same text and the same
// (second-resolution) delay stamp keep separate rows; only groupchat history uses the delay hash.
func TestIDLessDMDelayNoHash(t *testing.T) {
	const msg = `<message type="chat" from="bob@example.com/p"><body>ok</body><delay xmlns="urn:xmpp:delay" from="example.com" stamp="2026-01-01T00:00:00Z"/></message>`
	var ids []networkid.MessageID
	for range 2 {
		x, got := msgHookClient()
		runHandler(t, x, msg)
		ids = append(ids, onlyMessage(t, got).id)
	}
	if !strings.HasPrefix(string(ids[0]), randIDPrefix) || ids[0] == ids[1] {
		t.Errorf("ids = %v, want two distinct random ids", ids)
	}
}

func TestCarbonIDLessUsesStanzaIDAndDelay(t *testing.T) {
	const tmpl = `<message from="me@example.com"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat"><body>hi</body>%s</message></forwarded></received></message>`
	x, got := msgHookClient()
	runHandler(t, x, fmt.Sprintf(tmpl, `<stanza-id xmlns="urn:xmpp:sid:0" by="me@example.com" id="ARCH9"/><delay xmlns="urn:xmpp:delay" from="example.com" stamp="2026-01-01T00:00:00Z"/>`))
	m := onlyMessage(t, got)
	want, _ := time.Parse(time.RFC3339, "2026-01-01T00:00:00Z")
	if m.id != "xmp:sid:me@example.com:ARCH9" || !m.timestamp.Equal(want) {
		t.Errorf("id = %q timestamp = %v", m.id, m.timestamp)
	}
	x, got = msgHookClient()
	runHandler(t, x, fmt.Sprintf(tmpl, ``))
	if m := onlyMessage(t, got); !strings.HasPrefix(string(m.id), randIDPrefix) {
		t.Errorf("id = %q", m.id)
	}
}
