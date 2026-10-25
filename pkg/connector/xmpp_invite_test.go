package connector

import (
	"context"
	"encoding/xml"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const (
	invRoom    = "room@muc.example.com"
	invSecretR = "SECRETREASON"
	invSecretP = "SECRETPASS"
)

func mediatedInvite(extra string) string {
	return `<message from="` + invRoom + `" to="me@example.com" id="i1"><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com/res"><reason>` + invSecretR + `</reason></invite><password>` + invSecretP + `</password></x>` + extra + `</message>`
}

func directInvite(extra string) string {
	return `<message from="bob@example.com/res" to="me@example.com" id="i2"><x xmlns="jabber:x:conference" jid="` + invRoom + `" password="` + invSecretP + `" reason="` + invSecretR + `"/>` + extra + `</message>`
}

func parseInvite(t *testing.T, stanza string) *mucInvite {
	t.Helper()
	r, start := startOfMessage(t, stanza)
	_, _, _, _, _, _, ids, err := parseMessageBodyOOBIDs(r)
	if err != nil {
		t.Fatal(err)
	}
	return ids.inviteFrom(attrValue(start.Attr, "from"))
}

func TestParseInviteForms(t *testing.T) {
	m := parseInvite(t, mediatedInvite(`<body>fallback</body>`))
	if m == nil || m.room != invRoom || m.inviter != "bob@example.com" || m.reason != invSecretR || m.password != invSecretP || m.direct {
		t.Fatalf("mediated: %+v", m)
	}
	d := parseInvite(t, directInvite(`<body>fallback</body>`))
	if d == nil || d.room != invRoom || d.inviter != "bob@example.com" || d.reason != invSecretR || d.password != invSecretP || !d.direct {
		t.Fatalf("direct: %+v", d)
	}
}

func TestParseInviteRejects(t *testing.T) {
	for name, s := range map[string]string{
		"bad room":               `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="not a jid"/></message>`,
		"room with resource":     `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="room@muc.example.com/nick"/></message>`,
		"room without local":     `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="muc.example.com"/></message>`,
		"oversized room":         `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="` + strings.Repeat("a", 300) + `@example.com"/></message>`,
		"nested not direct":      `<message from="bob@example.com/r"><wrap><x xmlns="jabber:x:conference" jid="room@muc.example.com"/></wrap></message>`,
		"nested mediated":        `<message from="room@muc.example.com"><wrap><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com"/></x></wrap></message>`,
		"mediated from occupant": `<message from="room@muc.example.com/nick"><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com"/></x></message>`,
		"status only":            `<message from="room@muc.example.com"><x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/></x></message>`,
	} {
		if m := parseInvite(t, s); m != nil {
			t.Errorf("%s: parsed %+v", name, m)
		}
	}
}

func TestParseInviteCaps(t *testing.T) {
	m := parseInvite(t, `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="`+invRoom+`" password="`+strings.Repeat("p", 300)+`" reason="`+strings.Repeat("r", 5000)+`"/></message>`)
	if m == nil || m.password != "" || len(m.reason) != maxInviteReasonBytes {
		t.Fatalf("caps: %+v", m)
	}
	m = parseInvite(t, `<message from="`+invRoom+`"><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com"><reason>`+strings.Repeat("r", 5000)+`</reason></invite></x></message>`)
	if m == nil || len(m.reason) != maxInviteReasonBytes {
		t.Fatalf("mediated caps: %+v", m)
	}
}

type inviteRig struct {
	x       *xmppClient
	mu      sync.Mutex
	notices []string
	done    chan struct{}
	queued  *int
	logs    *bmLogBuf
}

func newInviteRig() *inviteRig {
	x, got := hookClient()
	_ = got
	r := &inviteRig{x: x, done: make(chan struct{}, 16), logs: &bmLogBuf{}}
	x.log = zerolog.New(r.logs).Level(zerolog.TraceLevel)
	x.mucNoticeHook = func(_, text string) {
		r.mu.Lock()
		r.notices = append(r.notices, text)
		r.mu.Unlock()
		r.done <- struct{}{}
	}
	n := 0
	r.queued = &n
	x.queueHook = nil
	return r
}

func (r *inviteRig) wait(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the invite notice")
	}
}

func (r *inviteRig) count() int {
	time.Sleep(50 * time.Millisecond)
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.notices)
}

func TestInviteNoticeOnceAndNotBridged(t *testing.T) {
	x, queued := hookClient()
	r := &inviteRig{x: x, done: make(chan struct{}, 16), logs: &bmLogBuf{}}
	x.log = zerolog.New(r.logs).Level(zerolog.TraceLevel)
	x.mucNoticeHook = func(_, text string) {
		r.mu.Lock()
		r.notices = append(r.notices, text)
		r.mu.Unlock()
		r.done <- struct{}{}
	}
	runHandler(t, x, mediatedInvite(`<body>FALLBACKBODY</body>`))
	r.wait(t)
	// The same room invited through the other form inside 10 minutes: no second notice.
	runHandler(t, x, directInvite(`<body>FALLBACKBODY</body>`))
	if n := r.count(); n != 1 {
		t.Fatalf("notices = %d, want 1", n)
	}
	if len(*queued) != 0 {
		t.Fatalf("invite bridged as %d events", len(*queued))
	}
	text := r.notices[0]
	for _, want := range []string{"bob@example.com", invRoom, invSecretR, "!jabber join " + invRoom, "password"} {
		if !strings.Contains(text, want) {
			t.Errorf("notice %q lacks %q", text, want)
		}
	}
	if strings.Contains(text, invSecretP) {
		t.Errorf("notice leaks the password: %q", text)
	}
	// After the interval another invite notifies again.
	x.inviteMu.Lock()
	x.inviteSeen[strings.ToLower(invRoom)] = time.Now().Add(-inviteNoticeInterval - time.Second)
	x.inviteMu.Unlock()
	runHandler(t, x, directInvite(""))
	r.wait(t)
	// The password is used by the next join presence, but a bookmark password would win.
	var toks []xml.Token
	x.mucJoinTokensHook = func(_ string, tk []xml.Token) { toks = tk }
	x.mucSendJoinHook = func(string, string) error { return nil }
	if err := x.sendMUCJoin(context.Background(), invRoom, "me", nil); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tk := range toks {
		if cd, ok := tk.(xml.CharData); ok && string(cd) == invSecretP {
			found = true
		}
	}
	if !found {
		t.Error("join presence lacks the invite password")
	}
	if out := r.logs.String(); strings.Contains(out, invSecretP) || strings.Contains(out, invSecretR) || strings.Contains(out, "FALLBACKBODY") {
		t.Errorf("logs leak invite content:\n%s", out)
	}
	x.resetInvites()
	if x.invitePassword(invRoom) != "" {
		t.Error("invite password survived a session reset")
	}
}

func TestInviteIgnoredWhenJoinedOrCarbonOrMAM(t *testing.T) {
	r := newInviteRig()
	x := r.x
	x.mucOurOccupantJID = map[string]string{invRoom: invRoom + "/me"}
	runHandler(t, x, mediatedInvite(""))
	if n := r.count(); n != 0 {
		t.Fatalf("joined room notified %d times", n)
	}
	x.mucOurOccupantJID = nil
	// Received carbon of the same invite: never notifies.
	runHandler(t, x, `<message from="me@example.com" to="me@example.com/r"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/res" to="me@example.com" type="normal"><x xmlns="jabber:x:conference" jid="`+invRoom+`"/><body>FALLBACKBODY</body></message></forwarded></received></message>`)
	if n := r.count(); n != 0 {
		t.Fatalf("carbon notified %d times", n)
	}
	// Invite from our own account is ignored too.
	runHandler(t, x, `<message from="me@example.com/other"><x xmlns="jabber:x:conference" jid="`+invRoom+`"/></message>`)
	if n := r.count(); n != 0 {
		t.Fatalf("own invite notified %d times", n)
	}
}

func TestSanitizeInviteReason(t *testing.T) {
	got := sanitizeInviteReason("a\x00b\n*bold* [x](http://e)‮" + strings.Repeat("z", 400))
	if strings.ContainsAny(got, "\x00\n‮") || strings.Contains(got, "*bold*") || !strings.HasSuffix(got, "...") {
		t.Errorf("sanitize = %q", got)
	}
}
