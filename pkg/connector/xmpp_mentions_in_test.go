package connector

import (
	"reflect"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const (
	menRoom = "room@muc.example.com"
	menMe   = id.UserID("@me:example.org")
)

// newMentionTestClient builds a client in menRoom with our nick "alice", a seen occupant "bob" and a
// fake ghost formatter.
func newMentionTestClient() (*xmppClient, *bridgev2.Portal) {
	x := &xmppClient{
		loginID: "me@example.com",
		login:   &bridgev2.UserLogin{UserLogin: &database.UserLogin{UserMXID: menMe}},
	}
	x.mucOurOccupantJID = map[string]string{menRoom: menRoom + "/alice"}
	x.mucAv.noteGhost(menRoom + "/bob")
	x.formatGhostHook = func(u networkid.UserID) id.UserID {
		return id.UserID("@g_" + strings.NewReplacer("/", "_", "@", "_").Replace(string(u)) + ":example.org")
	}
	return x, &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: menRoom}}}
}

// mentionContent runs applyInboundMentions on a MUC message.
func mentionContent(x *xmppClient, p *bridgev2.Portal, body string, refs []mentionRef, fromMe bool) *event.MessageEventContent {
	c := &event.MessageEventContent{MsgType: event.MsgText, Body: body}
	x.applyInboundMentions(p, c, body, replyFallback{}, refs, fromMe, true)
	return c
}

// TestParseMentionRefs covers the XEP-0372 parser: valid, malformed, other types and the cap.
func TestParseMentionRefs(t *testing.T) {
	ids := parseIDs(t, `<message from="room@muc.example.com/bob" type="groupchat"><body>hi alice and more</body>
<reference xmlns="urn:xmpp:reference:0" type="mention" begin="3" end="8" uri="xmpp:room@muc.example.com/alice"/>
<reference xmlns="urn:xmpp:reference:0" type="data" begin="0" end="2" uri="xmpp:x@example.com"/>
<reference xmlns="urn:xmpp:reference:0" type="mention" begin="a" end="8" uri="xmpp:x@example.com"/>
<reference xmlns="urn:xmpp:reference:0" type="mention" begin="5" end="5" uri="xmpp:x@example.com"/>
<reference xmlns="urn:xmpp:reference:0" type="mention" begin="-1" end="5" uri="xmpp:x@example.com"/>
<reference xmlns="urn:xmpp:reference:0" type="mention" begin="1" end="5" uri="https://example.com"/>
<x xmlns="urn:test"><reference xmlns="urn:xmpp:reference:0" type="mention" begin="0" end="2" uri="xmpp:n@example.com"/></x>
</message>`)
	want := []mentionRef{{3, 8, "xmpp:room@muc.example.com/alice"}}
	if !reflect.DeepEqual(ids.Mentions, want) {
		t.Fatalf("got %+v want %+v", ids.Mentions, want)
	}
	var sb strings.Builder
	sb.WriteString(`<message from="room@muc.example.com/bob" type="groupchat"><body>hello</body>`)
	for i := 0; i < 30; i++ {
		sb.WriteString(`<reference xmlns="urn:xmpp:reference:0" type="mention" begin="0" end="1" uri="xmpp:x@example.com"/>`)
	}
	sb.WriteString(`</message>`)
	if got := len(parseIDs(t, sb.String()).Mentions); got != maxMentionRefs {
		t.Fatalf("cap: got %d", got)
	}
}

// TestParseMentionRefsLeadShift checks offsets follow the trimmed body.
func TestParseMentionRefsLeadShift(t *testing.T) {
	ids := parseIDs(t, `<message type="groupchat"><body>  hi bob</body>
<reference xmlns="urn:xmpp:reference:0" type="mention" begin="5" end="8" uri="xmpp:room@muc.example.com/bob"/>
<reference xmlns="urn:xmpp:reference:0" type="mention" begin="0" end="1" uri="xmpp:room@muc.example.com/bob"/></message>`)
	if !reflect.DeepEqual(ids.Mentions, []mentionRef{{3, 6, "xmpp:room@muc.example.com/bob"}}) {
		t.Fatalf("got %+v", ids.Mentions)
	}
}

// TestRefToOwnOccupantAndGhost covers reference targets, code point offsets and unknown occupants.
func TestRefToOwnOccupantAndGhost(t *testing.T) {
	x, p := newMentionTestClient()
	// Emoji and multibyte text before the mentions: offsets are code points, not bytes.
	body := "héllo 🎉 alice and bob"
	refs := []mentionRef{
		{begin: 8, end: 13, uri: "xmpp:room@muc.example.com/alice"},
		{begin: 18, end: 21, uri: "xmpp:room@muc.example.com/bob"},
		{begin: 0, end: 2, uri: "xmpp:room@muc.example.com/carol"}, // never seen: ignored
	}
	c := mentionContent(x, p, body, refs, false)
	if c.Body != body || c.Format != event.FormatHTML {
		t.Fatalf("content: %+v", c)
	}
	want := `héllo 🎉 <a href="https://matrix.to/#/@me:example.org">alice</a> and <a href="https://matrix.to/#/@g_room_muc.example.com_bob:example.org">bob</a>`
	if c.FormattedBody != want {
		t.Fatalf("got %q", c.FormattedBody)
	}
	if !reflect.DeepEqual(c.Mentions.UserIDs, []id.UserID{menMe, "@g_room_muc.example.com_bob:example.org"}) {
		t.Fatalf("mentions %+v", c.Mentions)
	}
	// Percent-encoded uri and the bare login JID also map to the user.
	c = mentionContent(x, p, "ping me", []mentionRef{{5, 7, "xmpp:me@example.com"}}, false)
	if c.Mentions == nil || c.Mentions.UserIDs[0] != menMe {
		t.Fatalf("bare jid: %+v", c)
	}
	c = mentionContent(x, p, "hey alice", []mentionRef{{4, 9, "xmpp:room@muc.example.com/al%69ce"}}, false)
	if c.Mentions == nil {
		t.Fatalf("encoded uri not resolved")
	}
	// Unknown occupant and a foreign room: no mention, content untouched.
	c = mentionContent(x, p, "hi carol", []mentionRef{{3, 8, "xmpp:room@muc.example.com/carol"}, {0, 2, "xmpp:other@muc.example.com/bob"}}, false)
	if c.Format != "" || c.FormattedBody != "" || c.Mentions != nil {
		t.Fatalf("unexpected mention: %+v", c)
	}
}

// TestRefRangesRejected covers out-of-range, empty and overlapping references.
func TestRefRangesRejected(t *testing.T) {
	m := id.UserID("@u:example.org")
	if f, u := buildInboundMentions("abc", []mentionRange{{0, 9, m}, {2, 2, m}, {-1, 2, m}}, "", "", nil); f != "" || u != nil {
		t.Fatalf("out of range accepted: %q %v", f, u)
	}
	f, u := buildInboundMentions("abcdef", []mentionRange{{0, 3, m}, {2, 5, "@v:example.org"}}, "", "", nil)
	if want := `<a href="https://matrix.to/#/@u:example.org">abc</a>def`; f != want || len(u) != 1 {
		t.Fatalf("overlap: %q %v", f, u)
	}
}

// TestOwnNickHeuristic covers whole-word, case-insensitive and multi-word nick matching.
func TestOwnNickHeuristic(t *testing.T) {
	x, p := newMentionTestClient()
	c := mentionContent(x, p, "Hey ALICE, and alice again", nil, false)
	want := `Hey <a href="https://matrix.to/#/@me:example.org">ALICE</a>, and <a href="https://matrix.to/#/@me:example.org">alice</a> again`
	if c.FormattedBody != want || len(c.Mentions.UserIDs) != 1 {
		t.Fatalf("got %q %+v", c.FormattedBody, c.Mentions)
	}
	if c := mentionContent(x, p, "alicecooper and malice", nil, false); c.Mentions != nil || c.FormattedBody != "" {
		t.Fatalf("substring matched: %+v", c)
	}
	x.mucOurOccupantJID[menRoom] = menRoom + "/bob"
	if c := mentionContent(x, p, "see bobcat", nil, false); c.Mentions != nil {
		t.Fatalf("bob in bobcat matched")
	}
	x.mucOurOccupantJID[menRoom] = menRoom + "/Al Ice (work)"
	c = mentionContent(x, p, "ping al ice (work)!", nil, false)
	if c.Mentions == nil || !strings.Contains(c.FormattedBody, ">al ice (work)</a>!") {
		t.Fatalf("spaced nick: %+v", c)
	}
}

// TestLeadingNickAddressing covers "nick: " addressing of a seen occupant.
func TestLeadingNickAddressing(t *testing.T) {
	x, p := newMentionTestClient()
	ghost := id.UserID("@g_room_muc.example.com_bob:example.org")
	for _, body := range []string{"bob: hello", "bob, hello", "bob:"} {
		c := mentionContent(x, p, body, nil, false)
		if c.Mentions == nil || c.Mentions.UserIDs[0] != ghost || !strings.HasPrefix(c.FormattedBody, `<a href="https://matrix.to/#/`+string(ghost)+`">bob</a>`) {
			t.Fatalf("%q: %+v", body, c)
		}
	}
	for _, body := range []string{"hello bob: hi", "bobby: hi", "bob hello", "carol: hi"} {
		if c := mentionContent(x, p, body, nil, false); c.Mentions != nil {
			t.Fatalf("%q mentioned", body)
		}
	}
}

// TestOwnMessageNotSelfMentioned checks messages from the bridged user never mention it.
func TestOwnMessageNotSelfMentioned(t *testing.T) {
	x, p := newMentionTestClient()
	if c := mentionContent(x, p, "note to alice", []mentionRef{{8, 13, "xmpp:room@muc.example.com/alice"}}, true); c.Mentions != nil || c.FormattedBody != "" {
		t.Fatalf("self mention: %+v", c)
	}
	if c := mentionContent(x, p, "alice is me", nil, true); c.Mentions != nil {
		t.Fatalf("self heuristic mention: %+v", c)
	}
}

// TestMentionHTMLEscapeAndNewline checks escaping of the surrounding text and <br> for newlines.
func TestMentionHTMLEscapeAndNewline(t *testing.T) {
	x, p := newMentionTestClient()
	c := mentionContent(x, p, "<b>&</b>\nalice \"q\"", nil, false)
	want := `&lt;b&gt;&amp;&lt;/b&gt;<br><a href="https://matrix.to/#/@me:example.org">alice</a> &#34;q&#34;`
	if c.FormattedBody != want {
		t.Fatalf("got %q", c.FormattedBody)
	}
	if c.Body != "<b>&</b>\nalice \"q\"" {
		t.Fatalf("body changed: %q", c.Body)
	}
}

// TestMentionFallbackShift checks references follow a stripped XEP-0428 reply fallback.
func TestMentionFallbackShift(t *testing.T) {
	orig := "> quoted\nhi bob"
	fb := replyFallback{Start: 0, End: 9, Valid: true}
	text := stripReplyFallback(orig, fb)
	if text != "hi bob" {
		t.Fatalf("strip: %q", text)
	}
	refs := []mentionRef{{3, 5, "xmpp:room@muc.example.com/bob"}, {12, 15, "xmpp:room@muc.example.com/bob"}}
	got := mapMentionRefs(orig, fb, text, refs)
	if len(got) != 1 || got[0].begin != 3 || got[0].end != 6 {
		t.Fatalf("got %+v", got)
	}
	// Through applyInboundMentions: the pill covers "bob" in the stripped text.
	x, p := newMentionTestClient()
	c := &event.MessageEventContent{MsgType: event.MsgText, Body: text}
	x.applyInboundMentions(p, c, orig, fb, []mentionRef{{12, 15, "xmpp:room@muc.example.com/bob"}}, false, true)
	if !strings.HasPrefix(c.FormattedBody, `hi <a href=`) || !strings.HasSuffix(c.FormattedBody, `>bob</a>`) {
		t.Fatalf("got %q", c.FormattedBody)
	}
	// A text rewritten some other way drops references.
	if got := mapMentionRefs("> q\nhi", replyFallback{}, "hi", refs); got != nil {
		t.Fatalf("refs kept: %+v", got)
	}
}

// TestNoMentionUnchanged checks content without mentions keeps no Format/FormattedBody/Mentions.
func TestNoMentionUnchanged(t *testing.T) {
	x, p := newMentionTestClient()
	c := mentionContent(x, p, "just <text> & more", nil, false)
	if c.Format != "" || c.FormattedBody != "" || c.Mentions != nil || c.Body != "just <text> & more" {
		t.Fatalf("changed: %+v", c)
	}
}

// TestDMMentionOnlyOwnJID checks one-to-one chats only honour a reference to our own JID, no heuristics.
func TestDMMentionOnlyOwnJID(t *testing.T) {
	x, p := newMentionTestClient()
	p.ID = "bob@example.com"
	c := &event.MessageEventContent{MsgType: event.MsgText, Body: "hi me alice"}
	x.applyInboundMentions(p, c, c.Body, replyFallback{}, []mentionRef{{3, 5, "xmpp:me@example.com"}}, false, false)
	if c.Mentions == nil || len(c.Mentions.UserIDs) != 1 || !strings.Contains(c.FormattedBody, ">me</a> alice") {
		t.Fatalf("got %+v", c)
	}
}
