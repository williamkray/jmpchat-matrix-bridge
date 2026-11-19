package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmlstream"
)

const outLogin = networkid.UserLoginID("me@example.com")

func matrixRow(xmppID, stanzaID string) *database.Message {
	return &database.Message{
		ID: "matrix:$evt", SenderID: networkid.UserID(outLogin), MXID: "$evt",
		Metadata: &MessageMetadata{XMPPMessageID: xmppID, StanzaID: stanzaID},
	}
}

func TestRetractionPayloadStanza(t *testing.T) {
	toks, err := xmlstream.ReadAll(retractionPayload("TARGET-1", "own-id"))
	if err != nil {
		t.Fatal(err)
	}
	var starts []xml.StartElement
	var body string
	inBody := false
	for _, tok := range toks {
		switch v := tok.(type) {
		case xml.StartElement:
			starts = append(starts, v)
			inBody = v.Name.Local == "body"
		case xml.CharData:
			if inBody {
				body += string(v)
			}
		case xml.EndElement:
			inBody = false
		}
	}
	if len(starts) != 5 {
		t.Fatalf("want retract, fallback, body, store, origin-id; got %d elements", len(starts))
	}
	want := []struct{ space, local string }{
		{"urn:xmpp:message-retract:1", "retract"}, {"urn:xmpp:fallback:0", "fallback"},
		{"", "body"}, {"urn:xmpp:hints", "store"}, {"urn:xmpp:sid:0", "origin-id"},
	}
	for i, w := range want {
		if starts[i].Name.Space != w.space || starts[i].Name.Local != w.local {
			t.Errorf("element %d = %v, want %v", i, starts[i].Name, w)
		}
	}
	if attrValue(starts[0].Attr, "id") != "TARGET-1" || attrValue(starts[0].Attr, "xmlns") != "urn:xmpp:message-retract:1" {
		t.Errorf("retract attrs: %+v", starts[0].Attr)
	}
	if attrValue(starts[1].Attr, "for") != "urn:xmpp:message-retract:1" {
		t.Errorf("fallback attrs: %+v", starts[1].Attr)
	}
	if attrValue(starts[4].Attr, "id") != "own-id" {
		t.Errorf("origin-id attrs: %+v", starts[4].Attr)
	}
	if body != retractionFallbackBody {
		t.Errorf("body = %q", body)
	}
}

// The outgoing stanza must be recognised as a retraction by our own parser (round trip), and its
// fallback body must not surface.
func TestRetractionPayloadParsesBack(t *testing.T) {
	toks, _ := xmlstream.ReadAll(retractionPayload("T", "o"))
	buf := &bytes.Buffer{}
	enc := xml.NewEncoder(buf)
	_ = enc.EncodeToken(xml.StartElement{Name: xml.Name{Local: "message"}, Attr: []xml.Attr{{Name: xml.Name{Local: "type"}, Value: "chat"}}})
	for _, tok := range toks {
		if se, ok := tok.(xml.StartElement); ok {
			var attrs []xml.Attr // let the encoder derive xmlns from Name.Space
			for _, a := range se.Attr {
				if a.Name.Local != "xmlns" {
					attrs = append(attrs, a)
				}
			}
			se.Attr = attrs
			tok = se
		}
		_ = enc.EncodeToken(tok)
	}
	_ = enc.EncodeToken(xml.EndElement{Name: xml.Name{Local: "message"}})
	_ = enc.Flush()
	raw := buf.String()
	d := xml.NewDecoder(strings.NewReader(raw))
	d.Token() // <message>
	_, _, _, _, _, _, ids, err := parseMessageBodyOOBIDs(d)
	if err != nil {
		t.Fatalf("parse: %v (%s)", err, raw)
	}
	if ids.Retract == nil || ids.Retract.Target != "T" {
		t.Fatalf("not parsed as retraction of T: %+v (%s)", ids.Retract, raw)
	}
}

func TestIsOwnMessage(t *testing.T) {
	for _, tc := range []struct {
		sender string
		want   bool
	}{
		{string(outLogin), true},
		{"ME@Example.com", true},
		{"me@example.com/phone", true},
		{"bob@example.com", false},
		{"room@muc.example.com/me", false},
		{"", false},
	} {
		m := &database.Message{SenderID: networkid.UserID(tc.sender)}
		if got := isOwnMessage(m, outLogin); got != tc.want {
			t.Errorf("isOwnMessage(%q) = %v, want %v", tc.sender, got, tc.want)
		}
	}
	if isOwnMessage(nil, outLogin) {
		t.Error("nil row must not be own")
	}
}

func TestXMPPIDForMessage(t *testing.T) {
	for _, tc := range []struct {
		name string
		m    *database.Message
		want string
		ok   bool
	}{
		{"xmpp origin", &database.Message{ID: "abc"}, "abc", true},
		{"origin-id preferred", &database.Message{ID: "abc", Metadata: &MessageMetadata{OriginID: "orig"}}, "orig", true},
		{"matrix with meta", matrixRow("x1", ""), "x1", true},
		{"matrix without meta", &database.Message{ID: "matrix:$e"}, "", false},
		{"matrix empty meta", &database.Message{ID: "matrix:$e", Metadata: &MessageMetadata{}}, "", false},
		{"empty id", &database.Message{}, "", false},
	} {
		got, ok := xmppIDForMessage(tc.m)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%s: got (%q,%v), want (%q,%v)", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

func TestResolveRetractRef(t *testing.T) {
	ctx := context.Background()
	var tbl mucEchoTable
	// DM
	if ref, err := resolveRetractRef(ctx, &tbl, "p", false, matrixRow("x1", ""), time.Millisecond); err != nil || ref != "x1" {
		t.Errorf("dm: %q %v", ref, err)
	}
	if _, err := resolveRetractRef(ctx, &tbl, "p", false, &database.Message{ID: "matrix:$e"}, time.Millisecond); !errors.Is(err, errRetractNoXMPPRef) {
		t.Errorf("dm without id: %v", err)
	}
	// MUC with stored stanza-id
	if ref, err := resolveRetractRef(ctx, &tbl, "r@muc", true, matrixRow("x1", "ROOM-1"), time.Millisecond); err != nil || ref != "ROOM-1" {
		t.Errorf("muc stored: %q %v", ref, err)
	}
	// MUC without stanza-id and no entry: no origin-id fallback
	if _, err := resolveRetractRef(ctx, &tbl, "r@muc", true, matrixRow("x1", ""), time.Millisecond); !errors.Is(err, errRetractNoMUCRef) {
		t.Errorf("muc no entry: %v", err)
	}
	// MUC with echo already in the table
	tbl.register("r@muc", "x2")
	tbl.matchEcho("r@muc", []string{"x2"}, "ROOM-2", "occ")
	if ref, err := resolveRetractRef(ctx, &tbl, "r@muc", true, matrixRow("x2", ""), time.Second); err != nil || ref != "ROOM-2" {
		t.Errorf("muc from entry: %q %v", ref, err)
	}
	// Echo confirmed but the room stamps no stanza-id: clear error
	tbl.register("r@muc", "x3")
	tbl.matchEcho("r@muc", []string{"x3"}, "", "")
	if _, err := resolveRetractRef(ctx, &tbl, "r@muc", true, matrixRow("x3", ""), time.Second); !errors.Is(err, errRetractNoMUCRef) {
		t.Errorf("no stanza-id: %v", err)
	}
}

func TestResolveRetractRefWaitsForEcho(t *testing.T) {
	var tbl mucEchoTable
	tbl.register("r@muc", "x1")
	go func() {
		time.Sleep(20 * time.Millisecond)
		tbl.matchEcho("r@muc", []string{"", "x1"}, "ROOM-9", "")
	}()
	ref, err := resolveRetractRef(context.Background(), &tbl, "R@muc", true, matrixRow("x1", ""), 2*time.Second)
	if err != nil || ref != "ROOM-9" {
		t.Fatalf("got %q %v", ref, err)
	}
}

func TestResolveRetractRefTimeout(t *testing.T) {
	var tbl mucEchoTable
	tbl.register("r@muc", "x1")
	start := time.Now()
	_, err := resolveRetractRef(context.Background(), &tbl, "r@muc", true, matrixRow("x1", ""), 30*time.Millisecond)
	if !errors.Is(err, errRetractNoMUCRef) {
		t.Fatalf("err = %v", err)
	}
	if d := time.Since(start); d < 25*time.Millisecond || d > time.Second {
		t.Errorf("waited %v", d)
	}
	// ctx cancel also ends the wait
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolveRetractRef(ctx, &tbl, "r@muc", true, matrixRow("x1", ""), time.Minute); !errors.Is(err, errRetractNoMUCRef) {
		t.Fatalf("ctx err = %v", err)
	}
}

func TestMUCEchoBeforePostSave(t *testing.T) {
	var tbl mucEchoTable
	tbl.register("room@muc", "id1")
	res := tbl.matchEcho("room@muc", []string{"id1", "other"}, "STANZA", "OCC")
	if !res.matched || res.persist {
		t.Fatalf("echo first: %+v", res)
	}
	sid, occ, have := tbl.postSave("room@muc", "id1", "$e")
	if !have || sid != "STANZA" || occ != "OCC" {
		t.Fatalf("postSave: %q %q %v", sid, occ, have)
	}
	meta := &MessageMetadata{XMPPMessageID: "id1"}
	if !applyEchoIDs(meta, sid, occ) || meta.StanzaID != "STANZA" || meta.OccupantID != "OCC" {
		t.Errorf("meta %+v", meta)
	}
	if applyEchoIDs(meta, sid, occ) {
		t.Error("second apply must report no change")
	}
}

func TestMUCEchoAfterPostSave(t *testing.T) {
	var tbl mucEchoTable
	tbl.register("room@muc", "id1")
	if _, _, have := tbl.postSave("room@muc", "id1", "$evt"); have {
		t.Fatal("no echo yet")
	}
	res := tbl.matchEcho("ROOM@muc", []string{"", "id1"}, "STANZA", "OCC")
	if !res.matched || !res.persist || res.mxid != id.EventID("$evt") {
		t.Fatalf("echo second: %+v", res)
	}
	// duplicate echo: matched, nothing to persist again
	if res := tbl.matchEcho("room@muc", []string{"id1"}, "X", ""); !res.matched || res.persist {
		t.Errorf("dup echo: %+v", res)
	}
	if sid, found, to := tbl.wait(context.Background(), "room@muc", "id1", time.Second); sid != "STANZA" || !found || to {
		t.Errorf("wait: %q %v %v", sid, found, to)
	}
}

func TestMUCEchoUnknownAndExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	tbl := mucEchoTable{now: func() time.Time { return now }}
	tbl.register("room@muc", "id1")
	if res := tbl.matchEcho("room@muc", []string{"never-sent"}, "S", ""); res.matched {
		t.Error("unregistered id must not match (issue #11 messages are not ours)")
	}
	if res := tbl.matchEcho("other@muc", []string{"id1"}, "S", ""); res.matched {
		t.Error("entries are room-scoped")
	}
	now = now.Add(mucEchoTTL + time.Second)
	if res := tbl.matchEcho("room@muc", []string{"id1"}, "S", ""); res.matched {
		t.Error("expired entry must not match")
	}
	tbl.register("room@muc", "id2") // sweeps id1
	if _, found, _ := tbl.wait(context.Background(), "room@muc", "id1", time.Millisecond); found {
		t.Error("expired entry must be swept")
	}
	tbl.discard("room@muc", "id2")
	if _, found, _ := tbl.wait(context.Background(), "room@muc", "id2", time.Millisecond); found {
		t.Error("discarded entry must be gone")
	}
}

func TestHandleOwnMUCEchoOccupantGuard(t *testing.T) {
	x := &xmppClient{loginID: outLogin, mucOurOccupantJID: map[string]string{"room@muc": "room@muc/me"}}
	x.mucEcho.register("room@muc", "id1")
	ids := messageIDs{OriginID: "id1", StanzaIDs: []stanzaIDRef{{By: "room@muc", ID: "S1"}}}
	if x.handleOwnMUCEcho("room@muc", "room@muc/mallory", "zzz", ids) {
		t.Fatal("another occupant reusing our id must not be treated as our echo")
	}
	if !x.handleOwnMUCEcho("room@muc", "room@muc/me", "zzz", ids) {
		t.Fatal("our occupant's echo by origin-id must match")
	}
	if sid, _, _ := x.mucEcho.wait(context.Background(), "room@muc", "id1", time.Second); sid != "S1" {
		t.Errorf("stanza id = %q", sid)
	}
	// A stanza-id from an untrusted `by` is never captured.
	x.mucEcho.register("room@muc", "id2")
	bad := messageIDs{StanzaIDs: []stanzaIDRef{{By: "evil@muc", ID: "S2"}}}
	if !x.handleOwnMUCEcho("room@muc", "room@muc/me", "id2", bad) {
		t.Fatal("echo by id attr must match")
	}
	if sid, _, _ := x.mucEcho.wait(context.Background(), "room@muc", "id2", time.Second); sid != "" {
		t.Errorf("untrusted stanza-id captured: %q", sid)
	}
}

func TestCheogramRedactionRefusedWithNotice(t *testing.T) {
	for _, portalID := range []string{"+15551234567@cheogram.com", "+15551234567,+15557654321@cheogram.com", "123;phone-context=ca-us.phone-context.soprani.ca@cheogram.com"} {
		var notices []string
		x := &xmppClient{loginID: outLogin, noticeHook: func(_ *bridgev2.Portal, text string) { notices = append(notices, text) }}
		portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID(portalID), Receiver: outLogin}}}
		msg := &bridgev2.MatrixMessageRemove{TargetMessage: matrixRow("x1", "")}
		msg.Portal = portal
		// No session: any attempt to send would fail with "not connected", so nil proves nothing was sent.
		if err := x.handleMatrixMessageRemove(context.Background(), msg); err != nil {
			t.Fatalf("%s: want nil (redaction stands), got %v", portalID, err)
		}
		if len(notices) != 1 || notices[0] != smsCannotUnsendNotice {
			t.Errorf("%s: notices = %q", portalID, notices)
		}
	}
}

func TestRedactionOfOthersRefused(t *testing.T) {
	var notices []string
	x := &xmppClient{loginID: outLogin, noticeHook: func(_ *bridgev2.Portal, text string) { notices = append(notices, text) }}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "+15551234567@cheogram.com", Receiver: outLogin}}}
	msg := &bridgev2.MatrixMessageRemove{TargetMessage: &database.Message{ID: "abc", SenderID: "bob@example.com"}}
	msg.Portal = portal
	err := x.handleMatrixMessageRemove(context.Background(), msg)
	if !errors.Is(err, errRetractNotOwn) {
		t.Fatalf("err = %v", err)
	}
	if len(notices) != 0 {
		t.Errorf("no SMS notice for foreign messages: %q", notices)
	}
}

func TestRedactionNotConnected(t *testing.T) {
	x := &xmppClient{loginID: outLogin}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "bob@example.com", Receiver: outLogin}, RoomType: database.RoomTypeDM}}
	msg := &bridgev2.MatrixMessageRemove{TargetMessage: matrixRow("x1", "")}
	msg.Portal = portal
	if err := x.handleMatrixMessageRemove(context.Background(), msg); !errors.Is(err, errRetractNotConnected) {
		t.Fatalf("err = %v", err)
	}
}

func TestOutboundTombstoneRefs(t *testing.T) {
	row := withStanzaID(matrixRow("x1", ""), "ROOM-1")
	if targetMeta(row).StanzaID != "ROOM-1" {
		t.Fatal("stanza id not merged")
	}
	if targetMeta(matrixRow("x1", "")).StanzaID != "" {
		t.Fatal("original row mutated")
	}
	have := map[retractedRef]bool{}
	for _, r := range retractedRefsOf([]*database.Message{row}) {
		have[r] = true
	}
	if !have[retractedRef{retractScopeStanza, "ROOM-1"}] {
		t.Errorf("no stanza-scope tombstone: %v", have)
	}
	for _, sc := range senderScopes(string(outLogin), "") {
		if !have[retractedRef{sc, "x1"}] {
			t.Errorf("no tombstone for own id under %s", sc)
		}
	}
}

func TestRetractionCapabilities(t *testing.T) {
	if jabberCaps.Delete != event.CapLevelFullySupported {
		t.Errorf("Delete = %v", jabberCaps.Delete)
	}
	if jabberCapID != "github.com/mautrix/jabber.capabilities.2026_11b" || jabberCaps.ID != jabberCapID {
		t.Errorf("cap ID = %q / %q", jabberCapID, jabberCaps.ID)
	}
	if info, caps := (&JabberConnector{}).GetBridgeInfoVersion(); info != 2 || caps != 4 {
		t.Errorf("bridge info version = %d, %d", info, caps)
	}
	var _ bridgev2.RedactionHandlingNetworkAPI = (*xmppClient)(nil)
}

func TestRelayedRedactionOnlyOwn(t *testing.T) {
	x := &xmppClient{loginID: outLogin}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "bob@example.com", Receiver: outLogin}, RoomType: database.RoomTypeDM}}
	row := matrixRow("x1", "")
	row.SenderMXID = "@alice:example.org"
	msg := &bridgev2.MatrixMessageRemove{TargetMessage: row}
	msg.Portal = portal
	msg.OrigSender = &bridgev2.OrigSender{}
	msg.Event = &event.Event{Sender: "@mallory:example.org"}
	if err := x.handleMatrixMessageRemove(context.Background(), msg); !errors.Is(err, errRetractNotOwn) {
		t.Fatalf("other relayed user: err = %v", err)
	}
	msg.Event = &event.Event{Sender: "@alice:example.org"}
	if err := x.handleMatrixMessageRemove(context.Background(), msg); !errors.Is(err, errRetractNotConnected) {
		t.Fatalf("own relayed message should pass the ownership check: err = %v", err)
	}
}
