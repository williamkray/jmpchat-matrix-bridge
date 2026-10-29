package connector

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestPlanCardDAVSyncNamesUnnamedEntries(t *testing.T) {
	contacts := []cardContact{
		{ID: "1", Name: "Acme Concierge", Numbers: []cardNumber{{"+15559990100", "work"}}},
		{ID: "2", Name: "Ann", Numbers: []cardNumber{{"+15550000001", "mobile"}}},
		{ID: "3", Name: "Blank", Numbers: []cardNumber{{"+15550000002", "mobile"}}},
		{ID: "4", Name: "Multi", Numbers: []cardNumber{{"+15550000003", "mobile"}, {"+15550000004", "work"}}},
	}
	roster := map[string]string{
		"+15559990100@cheogram.com": "",      // chatted before, no name
		"+15550000001@cheogram.com": "Annie", // named: never renamed
		"+15550000002@cheogram.com": "  ",    // whitespace only counts as no name
		"+15550000003@cheogram.com": "",
	}
	p := planCardDAVSync(contacts, roster, nil)
	var got []string
	for _, a := range p.Names {
		if !a.Fill {
			t.Errorf("name entry without Fill: %+v", a)
		}
		got = append(got, a.Name+"="+a.E164)
	}
	want := []string{"Acme Concierge=+15559990100", "Blank=+15550000002", "Multi (mobile)=+15550000003"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("names = %v, want %v", got, want)
	}
	if adds := names(p); !reflect.DeepEqual(adds, []string{"Multi (work)=+15550000004"}) {
		t.Errorf("adds = %v", adds)
	}
	if p.AlreadyInRoster != 1 {
		t.Errorf("already = %d, want 1", p.AlreadyInRoster)
	}
	w := p.writes()
	if len(w) != 4 || !w[0].Fill || w[3].Fill {
		t.Errorf("writes must list names first, then adds: %+v", w)
	}
}

func TestApplyPlanFillsNames(t *testing.T) {
	list := adds(4)
	for i := range list[:3] {
		list[i].Fill = true
	}
	f := &fakeExec{
		loggedIn: true,
		unnamed:  map[string]bool{list[0].JID: true, list[3].JID: true}, // list[3]: planned add, appeared unnamed
		inRoster: map[string]bool{list[1].JID: true},                    // named meanwhile
		// list[2] was removed from the roster meanwhile
	}
	res := applyPlan(context.Background(), f.env(0), list)
	if res.Named != 2 || res.Added != 0 || res.Already != 1 || res.Gone != 1 || res.Outcome != outcomeDone {
		t.Fatalf("result: %+v", res)
	}
	set := strings.Join(f.set, ",")
	if !strings.Contains(set, list[0].JID+"=C0") || !strings.Contains(set, list[3].JID+"=C3") ||
		strings.Contains(set, list[1].JID) || strings.Contains(set, list[2].JID) {
		t.Fatalf("set = %v", f.set)
	}
}

func TestCardDAVPreviewMentionsNames(t *testing.T) {
	j := &cardDAVSyncJob{prefix: "!jc"}
	p := cardDAVPlan{
		Read:  2,
		Names: []rosterAdd{{JID: "+15559990100@cheogram.com", Name: "Acme", E164: "+15559990100", Label: "work", Fill: true}},
		Adds:  []rosterAdd{{JID: "+15550000001@cheogram.com", Name: "Ann", E164: "+15550000001", Label: "mobile"}},
	}
	got := j.previewText(p)
	for _, want := range []string{
		"2 contacts read, 1 numbers would be added, 1 unnamed roster entry would get a name, 0 already in your roster",
		"- Acme: +15559990100 (work), names the existing roster entry",
		"- Ann: +15550000001 (mobile)\n",
		"`!jc carddav-sync apply` to apply them.",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("preview lacks %q:\n%s", want, got)
		}
	}
	got = j.previewText(cardDAVPlan{Read: 1, AlreadyInRoster: 1})
	if strings.Contains(got, "unnamed") || !strings.Contains(got, "Nothing to add.") {
		t.Errorf("empty preview: %q", got)
	}
	sum := j.summaryText(p, applyResult{Named: 2, Outcome: outcomeDone})
	if !strings.Contains(sum, "0 numbers added, 2 unnamed roster entries named, 0 already") {
		t.Errorf("summary: %q", sum)
	}
}

func TestSearchRosterIncludesUnnamedAndDigits(t *testing.T) {
	x := &xmppClient{rosterLoaded: true}
	x.applyRosterItems(true,
		rosterEntry{jid: "+15557770100@cheogram.com"},
		rosterEntry{jid: "ann@example.com", name: "Ann"},
	)
	search := func(q string) []string {
		res, err := x.SearchRoster(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range res {
			out = append(out, r.JID+"|"+r.Name)
		}
		sort.Strings(out)
		return out
	}
	for q, want := range map[string][]string{
		"5557770100":        {"+15557770100@cheogram.com|"},
		"555-777-0100":      {"+15557770100@cheogram.com|"},
		"+1 (555) 777 0100": {"+15557770100@cheogram.com|"},
		"cheogram":          {"+15557770100@cheogram.com|"},
		"ann":               {"ann@example.com|Ann"},
		"555-000":           nil,
	} {
		if got := search(q); !reflect.DeepEqual(got, want) {
			t.Errorf("search %q = %v, want %v", q, got, want)
		}
	}
}

func TestRosterGroupsKept(t *testing.T) {
	x := &xmppClient{}
	x.applyRosterItems(true, rosterEntry{jid: "a@example.com", groups: []string{"Work"}}, rosterEntry{jid: "b@example.com"})
	if g := x.rosterGroupsOf("A@example.com"); !reflect.DeepEqual(g, []string{"Work"}) {
		t.Fatalf("groups = %v", g)
	}
	if g := x.rosterGroupsOf("b@example.com"); g != nil {
		t.Fatalf("b groups = %v", g)
	}
	// A push without groups clears them; a removal forgets them.
	x.applyRosterItems(false, rosterEntry{jid: "a@example.com", name: "A"})
	if g := x.rosterGroupsOf("a@example.com"); g != nil {
		t.Fatalf("groups after push = %v", g)
	}
	x.applyRosterItems(false, rosterEntry{jid: "a@example.com", groups: []string{"X"}}, rosterEntry{jid: "a@example.com", remove: true})
	if _, ok := x.rosterGroups["a@example.com"]; ok {
		t.Fatal("removed entry kept groups")
	}
	if name, in := x.rosterEntryName("a@example.com"); in || name != "" {
		t.Fatalf("removed entry still in roster: %q %v", name, in)
	}
}

func TestApplyPlanCapCountsFills(t *testing.T) {
	list := adds(5)
	list[0].Fill, list[1].Fill = true, true
	f := &fakeExec{loggedIn: true, unnamed: map[string]bool{list[0].JID: true}} // list[1] removed meanwhile
	res := applyPlan(context.Background(), f.env(2), list)
	// list[0] named, list[1] gone (no budget used), list[2] added, cap reached before list[3].
	if res.Named != 1 || res.Gone != 1 || res.Added != 1 || res.Outcome != outcomeMaxAdds || res.Remaining != 2 {
		t.Fatalf("result: %+v", res)
	}
	j := &cardDAVSyncJob{}
	if sum := j.summaryText(cardDAVPlan{}, res); !strings.Contains(sum, "1 not named because they were removed") ||
		!strings.Contains(sum, "Made 2 roster changes") {
		t.Fatalf("summary: %q", sum)
	}
}
