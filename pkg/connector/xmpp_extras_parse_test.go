package connector

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

// extrasChildren carries every signal Phase A parses as a direct child of the message.
const extrasChildren = `<replace xmlns="urn:xmpp:message-correct:0" id="ORIG"/>` +
	`<composing xmlns="http://jabber.org/protocol/chatstates"/>` +
	`<markable xmlns="urn:xmpp:chat-markers:0"/>` +
	`<displayed xmlns="urn:xmpp:chat-markers:0" id="D1"/>` +
	`<request xmlns="urn:xmpp:receipts"/>` +
	`<received xmlns="urn:xmpp:receipts" id="R1"/>` +
	`<subject>Topic  here</subject>` +
	`<encrypted xmlns="eu.siacs.conversations.axolotl"/>`

func checkExtrasFixture(t *testing.T, name string, e messageExtras) {
	t.Helper()
	if e.Replace != "ORIG" || e.ChatState != "composing" || !e.Markable || e.Displayed != "D1" ||
		!e.ReceiptRequest || e.ReceiptReceived != "R1" || e.Encryption != "omemo" {
		t.Errorf("%s: extras = %+v", name, e)
	}
	if e.Subject == nil || *e.Subject != "Topic  here" {
		t.Errorf("%s: subject = %v", name, e.Subject)
	}
}

func TestExtrasLivePath(t *testing.T) {
	_, _, ids := parseFull(t, `<message type="chat" from="bob@example.com/p"><body>hi</body>`+extrasChildren+`</message>`)
	checkExtrasFixture(t, "live", ids.Extras)
}

func TestExtrasCarbonPath(t *testing.T) {
	stanza := `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="bob@example.com" type="chat" id="c1"><body>hi</body>` + extrasChildren + `</message></forwarded></sent></message>`
	r, _ := startOfMessage(t, stanza)
	if _, err := r.Token(); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, _, _, ids, err := parseInnerMessageFromCarbon(r)
	if err != nil {
		t.Fatal(err)
	}
	checkExtrasFixture(t, "carbon", ids.Extras)
}

func TestExtrasMAMPath(t *testing.T) {
	x := newOuterFromTestClient()
	ch := x.mam.register("q1", networkid.PortalKey{ID: "bob@example.com", Receiver: x.loginID}, false)
	r, start := startOfMessage(t, `<message to="me@example.com"><result xmlns="urn:xmpp:mam:2" queryid="q1" id="A1"><forwarded xmlns="urn:xmpp:forward:0"><delay xmlns="urn:xmpp:delay" stamp="2026-01-01T00:00:00Z"/><message xmlns="jabber:client" from="bob@example.com/p" to="me@example.com" type="chat" id="m1"><body>hi</body>`+extrasChildren+`</message></forwarded></result></message>`)
	if err := x.handleMessageWithOOB(r, &start); err != nil {
		t.Fatal(err)
	}
	select {
	case it := <-ch:
		checkExtrasFixture(t, "mam", it.Extras)
	default:
		t.Fatal("no item delivered")
	}
}

func TestExtrasDirectChildrenOnly(t *testing.T) {
	_, _, ids := parseFull(t, `<message type="chat"><wrap xmlns="urn:example:wrap"><replace xmlns="urn:xmpp:message-correct:0" id="X"/><displayed xmlns="urn:xmpp:chat-markers:0" id="Y"/><subject>no</subject><encrypted xmlns="urn:xmpp:omemo:2"/></wrap></message>`)
	if !reflect.DeepEqual(ids.Extras, messageExtras{}) {
		t.Errorf("nested elements recognized: %+v", ids.Extras)
	}
}

func TestExtrasNamespaceResolved(t *testing.T) {
	// Namespace inherited from a prefixed declaration on the message itself.
	_, _, ids := parseFull(t, `<message type="chat" xmlns:c="urn:xmpp:message-correct:0"><c:replace id="ORIG"/></message>`)
	if ids.Extras.Replace != "ORIG" {
		t.Errorf("prefixed replace = %q", ids.Extras.Replace)
	}
	// Carbons <received> and elements in a foreign namespace are not receipts/corrections.
	_, _, ids = parseFull(t, `<message type="chat"><received xmlns="urn:xmpp:carbons:2" id="R"/><replace xmlns="urn:example:other" id="R"/><displayed xmlns="urn:example:other" id="R"/></message>`)
	if !reflect.DeepEqual(ids.Extras, messageExtras{}) {
		t.Errorf("foreign namespaces recognized: %+v", ids.Extras)
	}
}

func TestExtrasSubjectEmptyVsAbsent(t *testing.T) {
	_, _, ids := parseFull(t, `<message type="groupchat"><subject/></message>`)
	if ids.Extras.Subject == nil || *ids.Extras.Subject != "" {
		t.Errorf("empty subject = %v", ids.Extras.Subject)
	}
	_, _, ids = parseFull(t, `<message type="groupchat"><subject></subject><body>x</body></message>`)
	if ids.Extras.Subject == nil || *ids.Extras.Subject != "" {
		t.Errorf("empty subject (open/close) = %v", ids.Extras.Subject)
	}
	_, _, ids = parseFull(t, `<message type="groupchat"><body>x</body></message>`)
	if ids.Extras.Subject != nil {
		t.Errorf("absent subject = %v", *ids.Extras.Subject)
	}
	_, _, ids = parseFull(t, `<message type="groupchat"><subject>  a  b  </subject><subject>second</subject></message>`)
	if ids.Extras.Subject == nil || *ids.Extras.Subject != "a  b" {
		t.Errorf("first subject = %v", ids.Extras.Subject)
	}
}

func TestExtrasSubjectCapped(t *testing.T) {
	long := strings.Repeat("é", maxSubjectRunes+500)
	_, _, ids := parseFull(t, `<message type="groupchat"><subject>`+long+`</subject></message>`)
	if ids.Extras.Subject == nil {
		t.Fatal("subject missing")
	}
	if n := len([]rune(*ids.Extras.Subject)); n != maxSubjectRunes {
		t.Errorf("subject runes = %d, want %d", n, maxSubjectRunes)
	}
}

func TestExtrasEncryptionVariants(t *testing.T) {
	for _, tc := range []struct{ name, child, want string }{
		{"omemo legacy", `<encrypted xmlns="eu.siacs.conversations.axolotl"/>`, "omemo"},
		{"omemo 2", `<encrypted xmlns="urn:xmpp:omemo:2"/>`, "omemo2"},
		{"xep-0027", `<x xmlns="jabber:x:encrypted">blob</x>`, "pgp"},
		{"xep-0373", `<openpgp xmlns="urn:xmpp:openpgp:0">blob</openpgp>`, "openpgp"},
		{"eme only", `<encryption xmlns="urn:xmpp:eme:0" namespace="urn:example:enc" name="X"/>`, "eme:urn:example:enc"},
		{"eme then concrete", `<encryption xmlns="urn:xmpp:eme:0" namespace="urn:example:enc"/><encrypted xmlns="urn:xmpp:omemo:2"/>`, "omemo2"},
		{"concrete then eme", `<encrypted xmlns="urn:xmpp:omemo:2"/><encryption xmlns="urn:xmpp:eme:0" namespace="urn:example:enc"/>`, "omemo2"},
		{"none", `<body>x</body>`, ""},
	} {
		_, _, ids := parseFull(t, `<message type="chat">`+tc.child+`</message>`)
		if ids.Extras.Encryption != tc.want {
			t.Errorf("%s: encryption = %q, want %q", tc.name, ids.Extras.Encryption, tc.want)
		}
	}
}

func TestExtrasIDLengthCap(t *testing.T) {
	long := strings.Repeat("a", maxStableIDLen+1)
	_, _, ids := parseFull(t, `<message type="chat"><replace xmlns="urn:xmpp:message-correct:0" id="`+long+`"/><displayed xmlns="urn:xmpp:chat-markers:0" id="`+long+`"/><received xmlns="urn:xmpp:receipts" id="`+long+`"/></message>`)
	if ids.Extras.Replace != "" || ids.Extras.Displayed != "" || ids.Extras.ReceiptReceived != "" {
		t.Errorf("over-long ids kept: %+v", ids.Extras)
	}
	_, _, ids = parseFull(t, `<message type="chat"><replace xmlns="urn:xmpp:message-correct:0" id="first"/><replace xmlns="urn:xmpp:message-correct:0" id="second"/></message>`)
	if ids.Extras.Replace != "first" {
		t.Errorf("first occurrence must win, got %q", ids.Extras.Replace)
	}
}

func TestDBMetaMarkableAndMediaFlags(t *testing.T) {
	m := &jabberRemoteMessage{id: "m1", markable: true}
	if meta := m.dbMeta(); !meta.Markable || meta.Media {
		t.Errorf("text meta = %+v", meta)
	}
	// A single mxc:// body converts to a file part without network access: Media is set, text is not.
	media := &jabberRemoteMessage{id: "m2", body: "mxc://example.com/abc"}
	cm, err := media.ConvertMessage(context.Background(), nil, nil)
	if err != nil || cm == nil || len(cm.Parts) != 1 {
		t.Fatalf("convert media: %v %v", cm, err)
	}
	if meta, ok := cm.Parts[0].DBMetadata.(*MessageMetadata); !ok || !meta.Media {
		t.Errorf("media part meta = %+v", cm.Parts[0].DBMetadata)
	}
	text := &jabberRemoteMessage{id: "m3", body: "hello", markable: true}
	cm, err = text.ConvertMessage(context.Background(), nil, nil)
	if err != nil || cm == nil || len(cm.Parts) != 1 {
		t.Fatalf("convert text: %v %v", cm, err)
	}
	if meta, ok := cm.Parts[0].DBMetadata.(*MessageMetadata); !ok || meta.Media || !meta.Markable {
		t.Errorf("text part meta = %+v", cm.Parts[0].DBMetadata)
	}
}
