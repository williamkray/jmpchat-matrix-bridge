package connector

import (
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

// jidSet is a roster of named entries.
func jidSet(jids ...string) map[string]string {
	m := map[string]string{}
	for _, j := range jids {
		m[j] = "Someone"
	}
	return m
}

func names(p cardDAVPlan) []string {
	var out []string
	for _, a := range p.Adds {
		out = append(out, a.Name+"="+a.E164)
	}
	return out
}

func TestPlanCardDAVSync(t *testing.T) {
	cn := func(id, name string, nums ...cardNumber) cardContact {
		return cardContact{ID: id, Name: name, Numbers: nums}
	}
	n := func(e, l string) cardNumber { return cardNumber{e, l} }
	for _, tc := range []struct {
		name     string
		contacts []cardContact
		roster   map[string]string
		want     []string
		already  int
		skipped  map[string]int
	}{
		{
			name:     "single number uses the base name",
			contacts: []cardContact{cn("1", "Ann", n("+15550000001", "mobile"))},
			want:     []string{"Ann=+15550000001"},
		},
		{
			name:     "multiple numbers get labels",
			contacts: []cardContact{cn("1", "John Doe", n("+15550000001", "mobile"), n("+15550000002", "work"))},
			want:     []string{"John Doe (mobile)=+15550000001", "John Doe (work)=+15550000002"},
		},
		{
			name:     "same label twice",
			contacts: []cardContact{cn("1", "J", n("+15550000001", "mobile"), n("+15550000002", "Mobile"), n("+15550000003", "mobile"))},
			want:     []string{"J (mobile)=+15550000001", "J (Mobile 2)=+15550000002", "J (mobile 3)=+15550000003"},
		},
		{
			name:     "custom label",
			contacts: []cardContact{cn("1", "John Doe", n("+15550000001", "Office"), n("+15550000002", "home"))},
			want:     []string{"John Doe (Office)=+15550000001", "John Doe (home)=+15550000002"},
		},
		{
			name:     "already in roster is skipped, never renamed",
			contacts: []cardContact{cn("1", "Ann", n("+15550000001", "mobile"))},
			roster:   jidSet("+15550000001@cheogram.com"),
			already:  1,
		},
		{
			name:     "partial roster, multi-number: label decision uses all numbers",
			contacts: []cardContact{cn("1", "John Doe", n("+15550000001", "mobile"), n("+15550000002", "work"))},
			roster:   jidSet("+15550000001@cheogram.com"),
			want:     []string{"John Doe (work)=+15550000002"},
			already:  1,
		},
		{
			name: "cross-contact duplicate: first in deterministic order claims it",
			contacts: []cardContact{
				cn("2", "Zed", n("+15550000001", "mobile")),
				cn("1", "Amy", n("+15550000001", "mobile"), n("+15550000009", "home")),
			},
			want:    []string{"Amy (mobile)=+15550000001", "Amy (home)=+15550000009"},
			skipped: map[string]int{reasonDuplicate: 1},
		},
		{
			name: "duplicate display names across contacts are kept",
			contacts: []cardContact{
				cn("1", "Sam", n("+15550000001", "mobile")),
				cn("2", "Sam", n("+15550000002", "mobile")),
			},
			want: []string{"Sam=+15550000001", "Sam=+15550000002"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := planCardDAVSync(tc.contacts, tc.roster, nil)
			if got := names(p); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("adds = %v, want %v", got, tc.want)
			}
			if p.AlreadyInRoster != tc.already {
				t.Errorf("already = %d, want %d", p.AlreadyInRoster, tc.already)
			}
			if p.Read != len(tc.contacts) {
				t.Errorf("read = %d", p.Read)
			}
			if len(p.Skipped) == 0 {
				p.Skipped = nil
			}
			if !reflect.DeepEqual(p.Skipped, tc.skipped) {
				t.Errorf("skipped = %v, want %v", p.Skipped, tc.skipped)
			}
		})
	}
}

func TestPlanCardDAVSyncNameTruncationAndJID(t *testing.T) {
	long := strings.Repeat("é", 100)
	c := []cardContact{{ID: "1", Name: long, Numbers: []cardNumber{{"+15550000001", "mobile"}, {"+15550000002", "work"}}}}
	p := planCardDAVSync(c, nil, nil)
	if len(p.Adds) != 2 {
		t.Fatalf("%+v", p)
	}
	for _, a := range p.Adds {
		if utf8.RuneCountInString(a.Name) > maxContactNameRunes || !strings.HasSuffix(a.Name, ")") {
			t.Errorf("name %q (%d runes)", a.Name, utf8.RuneCountInString(a.Name))
		}
		if a.JID != strings.ToLower(a.E164+"@cheogram.com") {
			t.Errorf("jid %q", a.JID)
		}
	}
	single := planCardDAVSync([]cardContact{{ID: "1", Name: long, Numbers: []cardNumber{{"+15550000001", "mobile"}}}}, nil, nil)
	if utf8.RuneCountInString(single.Adds[0].Name) != maxContactNameRunes {
		t.Errorf("single name runes = %d", utf8.RuneCountInString(single.Adds[0].Name))
	}
}

func TestPlanCardDAVSyncDeterministicAndMergesSkips(t *testing.T) {
	contacts := []cardContact{
		{ID: "b", Name: "bob", Numbers: []cardNumber{{"+15550000002", "mobile"}}},
		{ID: "a", Name: "Alice", Numbers: []cardNumber{{"+15550000001", "mobile"}}},
		{ID: "c", Name: "Carl", Numbers: []cardNumber{{"+15550000001", "mobile"}}},
	}
	first := planCardDAVSync(contacts, nil, map[string]int{reasonBadCard: 2})
	for i := 0; i < 20; i++ {
		rev := []cardContact{contacts[2], contacts[0], contacts[1]}
		if i%2 == 0 {
			rev = []cardContact{contacts[1], contacts[2], contacts[0]}
		}
		if got := planCardDAVSync(rev, nil, map[string]int{reasonBadCard: 2}); !reflect.DeepEqual(got, first) {
			t.Fatalf("non-deterministic:\n%+v\n%+v", got, first)
		}
	}
	if got := names(first); !reflect.DeepEqual(got, []string{"Alice=+15550000001", "bob=+15550000002"}) {
		t.Errorf("adds = %v", got)
	}
	if first.Skipped[reasonBadCard] != 2 || first.Skipped[reasonDuplicate] != 1 {
		t.Errorf("skipped = %v", first.Skipped)
	}
	if s := skippedSummary(first.Skipped); s != "1 duplicate, 2 bad_card" {
		t.Errorf("summary = %q", s)
	}
}
