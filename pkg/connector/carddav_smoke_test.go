package connector

import (
	"os"
	"strings"
	"testing"
)

// TestCardDAVSmoke runs discovery + fetch + planner against a real CardDAV server (for example the
// Radicale container of the local smoke test). It is skipped unless CARDDAV_SMOKE_URL is set, together
// with CARDDAV_SMOKE_USER and CARDDAV_SMOKE_PASS. http:// and private addresses are allowed here.
// CARDDAV_SMOKE_ROSTER_JID optionally names a JID already "in the roster" for the merge check.
func TestCardDAVSmoke(t *testing.T) {
	rawURL := os.Getenv("CARDDAV_SMOKE_URL")
	if rawURL == "" {
		t.Skip("CARDDAV_SMOKE_URL not set")
	}
	cfg := CardDAVConfig{AllowInsecureHTTP: true, AllowPrivateHosts: true, DefaultCountryCode: "1"}
	creds, books, err := validateCardDAVLogin(ctxT(t), cfg, rawURL, os.Getenv("CARDDAV_SMOKE_USER"), os.Getenv("CARDDAV_SMOKE_PASS"), "")
	if err != nil {
		t.Fatalf("discovery: %v", err)
	}
	for _, b := range books {
		t.Logf("book: %s", bookLabel(b))
	}
	s, err := fetchContacts(ctxT(t), cfg, creds)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	t.Logf("cards=%d contacts=%d books_ok=%d books_failed=%d bytes=%d skips=%v", s.Cards, len(s.Contacts), s.BooksOK, s.BooksFailed, s.Bytes, s.Skips)
	roster := map[string]string{}
	if j := strings.ToLower(os.Getenv("CARDDAV_SMOKE_ROSTER_JID")); j != "" {
		roster[j] = "Smoke"
	}
	p := planCardDAVSync(s.Contacts, roster, s.Skips)
	for _, a := range p.Adds {
		t.Logf("add: %-28s %s (%s)", a.Name, a.JID, a.Label)
	}
	t.Logf("plan: read=%d adds=%d names=%d already_in_roster=%d skipped=%q", p.Read, len(p.Adds), len(p.Names), p.AlreadyInRoster, skippedSummary(p.Skipped))
}
