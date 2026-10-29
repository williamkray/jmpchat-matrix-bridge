package connector

import (
	"fmt"
	"sort"
	"strings"
)

// rosterAdd is one planned roster entry.
type rosterAdd struct {
	JID   string // lowercased +E164@cheogram.com
	Name  string
	E164  string
	Label string
	Fill  bool // the JID is already in the roster without a name: only the name is set
}

// cardDAVPlan is the planner output: entries to add, unnamed entries to name, and counters.
type cardDAVPlan struct {
	Adds            []rosterAdd
	Names           []rosterAdd    // roster entries without a name that get the contact's name (Fill set)
	Read            int            // contacts considered
	AlreadyInRoster int            // numbers skipped because the JID is already in the roster with a name
	Skipped         map[string]int // by reason
}

// planCardDAVSync is pure and deterministic; the preview and `apply` run the same function.
// roster maps the lowercased bare JIDs of the roster to their roster name ("" = no name). A named entry
// is never renamed; an unnamed one (typically a number you chatted with before it was in the address
// book) gets the contact's name. extraSkips are counts from earlier stages (extraction, bad cards)
// merged into the result.
func planCardDAVSync(contacts []cardContact, roster map[string]string, extraSkips map[string]int) cardDAVPlan {
	p := cardDAVPlan{Read: len(contacts), Skipped: map[string]int{}}
	for k, v := range extraSkips {
		p.Skipped[k] += v
	}
	sorted := make([]cardContact, len(contacts))
	copy(sorted, contacts)
	sort.SliceStable(sorted, func(i, j int) bool {
		a, b := strings.ToLower(sorted[i].Name), strings.ToLower(sorted[j].Name)
		if a != b {
			return a < b
		}
		if sorted[i].ID != sorted[j].ID {
			return sorted[i].ID < sorted[j].ID
		}
		return sorted[i].Name < sorted[j].Name
	})
	claimed := map[string]struct{}{}
	for _, c := range sorted {
		// Numbers not claimed by an earlier contact; the labelling decision uses all of them,
		// including ones already in the roster.
		var mine []cardNumber
		for _, n := range c.Numbers {
			if _, dup := claimed[n.E164]; dup {
				p.Skipped[reasonDuplicate]++
				continue
			}
			claimed[n.E164] = struct{}{}
			mine = append(mine, n)
		}
		labelCount := map[string]int{}
		for _, n := range mine {
			jid := strings.ToLower(cheogramJID(n.E164))
			name := c.Name
			if len(mine) > 1 {
				key := strings.ToLower(n.Label)
				labelCount[key]++
				label := n.Label
				if labelCount[key] > 1 {
					label = fmt.Sprintf("%s %d", n.Label, labelCount[key])
				}
				suffix := " (" + label + ")"
				name = truncateRunes(c.Name, maxContactNameRunes-len([]rune(suffix))) + suffix
			} else {
				name = truncateRunes(name, maxContactNameRunes)
			}
			add := rosterAdd{JID: jid, Name: name, E164: n.E164, Label: n.Label}
			if cur, in := roster[jid]; in {
				if strings.TrimSpace(cur) != "" {
					p.AlreadyInRoster++
					continue
				}
				add.Fill = true
				p.Names = append(p.Names, add)
				continue
			}
			p.Adds = append(p.Adds, add)
		}
	}
	return p
}

// writes is everything apply writes, in order: names for unnamed entries first (they are chats that
// already exist), then adds.
func (p cardDAVPlan) writes() []rosterAdd {
	out := make([]rosterAdd, 0, len(p.Names)+len(p.Adds))
	return append(append(out, p.Names...), p.Adds...)
}

// skippedSummary renders skip counts as "3 no_number, 1 invalid" in a fixed order ("" if none).
func skippedSummary(skipped map[string]int) string {
	var parts []string
	for _, r := range cardDAVReasonOrder {
		if n := skipped[r]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, r))
		}
	}
	return strings.Join(parts, ", ")
}

// totalSkipped sums all skip counts.
func totalSkipped(skipped map[string]int) int {
	t := 0
	for _, n := range skipped {
		t += n
	}
	return t
}
