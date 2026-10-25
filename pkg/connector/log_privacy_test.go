package connector

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// Issue #12: message content (bodies, captions, reaction emoji, media URLs) must not appear in log output, at any
// level; only lengths and ids.
func TestInboundMessageContentNotLogged(t *testing.T) {
	const secret = "TOPSECRETBODY"
	x, _ := hookClient()
	x.conn = &JabberConnector{}
	buf := &bmLogBuf{}
	x.log = zerolog.New(buf).Level(zerolog.TraceLevel)
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}

	runHandler(t, x, `<message type="chat" from="bob@example.com/res" to="me@example.com" id="c1"><body>`+secret+` chat</body></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" to="me@example.com" id="c2"><body>`+secret+` oob</body><x xmlns="jabber:x:oob"><url>https://example.com/f.png</url></x></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" to="me@example.com" id="c3"><body>`+secret+` reply</body><reply xmlns="urn:xmpp:reply:0" id="c1"/></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="g1"><body>`+secret+` muc</body></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="g2"><body>`+secret+` mine</body></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="g3"><reactions xmlns="urn:xmpp:reactions:0" id="S1"><reaction>🦄</reaction></reactions></message>`)
	serveStanza(t, x, `<message xmlns="jabber:client" type="chat" from="bob@example.com/res" id="s1"><body>`+secret+` serve</body></message>`)
	serveStanza(t, x, `<message xmlns="jabber:client" type="groupchat" from="room@muc.example.com/bob" id="s2"><body>`+secret+` serve muc</body></message>`)

	// XEP-0308 corrections (live DM, MUC, carbon): the corrected text is content too.
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" to="me@example.com" id="k1"><body>`+secret+` fix dm</body><replace xmlns="urn:xmpp:message-correct:0" id="c1"/></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="k2"><body>`+secret+` fix muc</body><replace xmlns="urn:xmpp:message-correct:0" id="g1"/></message>`)
	runHandler(t, x, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="bob@example.com" type="chat" id="k3"><body>`+secret+` fix carbon</body><replace xmlns="urn:xmpp:message-correct:0" id="c1"/></message></forwarded></sent></message>`)

	// MUC subject (room topic) and encrypted messages (OMEMO fallback body, aesgcm URL): content too.
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="t1"><subject>`+secret+` subject</subject></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" id="t2"><encrypted xmlns="eu.siacs.conversations.axolotl"><payload>AA</payload></encrypted><body>`+secret+` omemo fallback</body></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" id="t3"><body>aesgcm://example.com/`+secret+`#key</body></message>`)

	// XEP-0085/0333/0184 signals (DM, MUC, carbons), also with content in an unrelated <body>.
	x.mucAv.noteGhost("room@muc.example.com/bob")
	x.signalHook = func(context.Context, outSignal) error { return nil }
	x.bridgedHook = func(networkid.PortalKey, networkid.MessageID) bool { return true }
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" id="n1"><composing xmlns="http://jabber.org/protocol/chatstates"/></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" id="n2"><displayed xmlns="urn:xmpp:chat-markers:0" id="c1"/><body>`+secret+` marker</body></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" id="n3"><received xmlns="urn:xmpp:receipts" id="c1"/></message>`)
	runHandler(t, x, `<message type="chat" from="bob@example.com/res" id="n4"><body>`+secret+` request</body><request xmlns="urn:xmpp:receipts"/><markable xmlns="urn:xmpp:chat-markers:0"/></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="n5"><displayed xmlns="urn:xmpp:chat-markers:0" id="S1"/></message>`)
	runHandler(t, x, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="bob@example.com" type="chat" id="n6"><displayed xmlns="urn:xmpp:chat-markers:0" id="c1"/><body>`+secret+` carbon marker</body></message></forwarded></sent></message>`)
	_ = x.HandleMatrixTyping(context.Background(), &bridgev2.MatrixTyping{Portal: &bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey("bob@example.com"), RoomType: database.RoomTypeDM}}, IsTyping: true})
	_ = x.HandleMatrixReadReceipt(context.Background(), readMsg(&bridgev2.Portal{Portal: &database.Portal{PortalKey: sigKey("bob@example.com"), RoomType: database.RoomTypeDM}}, mkRow("m1", "bob@example.com", 5, true, "")))

	// MUC invites (mediated, direct, carbon): reason, password and fallback body are content.
	runHandler(t, x, `<message from="room2@muc.example.com"><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com"><reason>`+secret+` reason</reason></invite><password>`+secret+`pw</password></x><body>`+secret+` fb</body></message>`)
	runHandler(t, x, `<message from="bob@example.com/res" type="normal"><x xmlns="jabber:x:conference" jid="room3@muc.example.com" reason="`+secret+` r2" password="`+secret+`pw2"/><body>`+secret+` fb2</body></message>`)
	runHandler(t, x, `<message from="me@example.com"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/res" to="me@example.com" type="normal"><x xmlns="jabber:x:conference" jid="room4@muc.example.com" reason="`+secret+` r3"/><body>`+secret+` fb3</body></message></forwarded></received></message>`)
	time.Sleep(100 * time.Millisecond)

	out := buf.String()
	if out == "" {
		t.Fatal("nothing was logged; the test proves nothing")
	}
	for _, leaked := range []string{secret, "🦄", "example.com/f.png"} {
		if strings.Contains(out, leaked) {
			t.Errorf("logs leak message content %q:\n%s", leaked, out)
		}
	}
	if strings.Contains(out, "body_preview") {
		t.Errorf("body_preview is still logged:\n%s", out)
	}
}
