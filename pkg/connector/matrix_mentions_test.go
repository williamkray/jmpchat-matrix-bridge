package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

const (
	testAliceMXID = "@jabber_room=40conf.example.com=2fAlice:example.org"
	testBobMXID   = "@jabber_room=40conf.example.com=2fBob:example.org"
	testRealMXID  = "@carol:example.org"
)

// testMentionResolve resolves the two test ghosts to their nicks and leaves other users alone.
func testMentionResolve(u id.UserID) (string, bool) {
	switch u {
	case testAliceMXID:
		return "Alice", true
	case testBobMXID:
		return "Bob", true
	}
	return "", false
}

func pill(mxid, name string) string {
	return `<a href="https://matrix.to/#/` + mxid + `">` + name + `</a>`
}

// TestRewriteMentions covers the pure body rewrite.
func TestRewriteMentions(t *testing.T) {
	tests := []struct {
		name, body, html string
		isHTML           bool
		want             string
	}{
		{"nick", "Alice (jabber): hi", pill(testAliceMXID, "Alice (jabber)") + ": hi", true, "Alice: hi"},
		{"two", "Alice (jabber), Bob (jabber) hi", pill(testAliceMXID, "Alice (jabber)") + ", " + pill(testBobMXID, "Bob (jabber)") + " hi", true, "Alice, Bob hi"},
		{"non ghost", "Carol: hi", pill(testRealMXID, "Carol") + ": hi", true, "Carol: hi"},
		{"no html", "Alice (jabber): hi", "", true, "Alice (jabber): hi"},
		{"not html format", "Alice (jabber): hi", pill(testAliceMXID, "Alice (jabber)"), false, "Alice (jabber): hi"},
		{"code untouched", "Alice (jabber) `x*y`", pill(testAliceMXID, "Alice (jabber)") + " <code>x*y</code>", true, "Alice `x*y`"},
		{"html only", "", "<p>" + pill(testAliceMXID, "Alice (jabber)") + ": hi</p>", true, "Alice: hi"},
		{"html only non ghost", "", "<p>" + pill(testRealMXID, "Carol") + ": hi</p>", true, "Carol: hi"},
		{"matrix uri", "Alice (jabber): hi", `<a href="matrix:u/jabber_room=40conf.example.com=2fAlice:example.org">Alice (jabber)</a>: hi`, true, "Alice: hi"},
		{"name absent", "hello " + testAliceMXID, pill(testAliceMXID, "Alice (jabber)") + " hello", true, "hello Alice"},
		{"name absent unchanged", "hello", pill(testAliceMXID, "Alice (jabber)") + " hello", true, "hello"},
	}
	for _, tc := range tests {
		got := rewriteMentions(tc.body, tc.html, tc.isHTML, testMentionResolve)
		if got != tc.want {
			t.Errorf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

// TestMentionResolver covers ghost ID mapping with the parse seam.
func TestMentionResolver(t *testing.T) {
	x := &xmppClient{
		loginID: "me@example.com",
		roster:  map[string]string{"friend@example.com": "Friend Name"},
	}
	ghosts := map[id.UserID]networkid.UserID{
		"@g_occ:example.org":     "room@conf.example.com/Nick Name",
		"@g_roster:example.org":  "friend@example.com",
		"@g_unknown:example.org": "stranger@example.com",
		"@g_me:example.org":      "me@example.com",
		"@g_num:example.org":     "+15551234567@cheogram.com",
	}
	x.parseGhostHook = func(u id.UserID) (networkid.UserID, bool) {
		v, ok := ghosts[u]
		return v, ok
	}
	resolve := x.mentionResolver(context.Background(), nil)
	want := map[id.UserID]string{
		"@g_occ:example.org":     "Nick Name",
		"@g_roster:example.org":  "Friend Name",
		"@g_unknown:example.org": "stranger",
		"@g_me:example.org":      "me",
		"@g_num:example.org":     "+15551234567",
	}
	for u, w := range want {
		if got, ok := resolve(u); !ok || got != w {
			t.Errorf("%s: got %q,%v want %q", u, got, ok, w)
		}
	}
	if _, ok := resolve("@carol:example.org"); ok {
		t.Error("non-ghost resolved")
	}
	// Without a hook and without a portal nothing resolves.
	y := &xmppClient{}
	if _, ok := y.mentionResolver(context.Background(), nil)("@g_occ:example.org"); ok {
		t.Error("nil portal resolved")
	}
}
