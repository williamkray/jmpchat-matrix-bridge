package connector

import (
	"context"
	"sort"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/format"
	"maunium.net/go/mautrix/id"
)

// mentionPill is one user pill (mention) found in a Matrix formatted_body.
type mentionPill struct {
	displayname string
	mxid        id.UserID
}

// newMentionParser returns an HTML parser with the settings of format.HTMLToText whose PillConverter
// calls onPill for every user pill and renders the pill with the text it returns.
func newMentionParser(onPill func(displayname string, mxid id.UserID) string) *format.HTMLParser {
	return &format.HTMLParser{
		TabsToSpaces:   4,
		Newline:        "\n",
		HorizontalLine: "\n---\n",
		PillConverter: func(displayname, mxid, eventID string, ctx format.Context) string {
			if len(mxid) > 0 && mxid[0] == '@' {
				if s := onPill(displayname, id.UserID(mxid)); s != "" || displayname == "" {
					return s
				}
				return displayname
			}
			return format.DefaultPillConverter(displayname, mxid, eventID, ctx)
		},
	}
}

// rewriteMentions makes the XMPP body of a Matrix message use the XMPP-side names of mentioned bridged
// users instead of their Matrix display names (which carry the appservice suffix). Without HTML
// formatting the body is returned unchanged. Otherwise each user pill that resolve knows is replaced in
// body (display name and raw user ID); an empty body (HTML-only event) is derived from the formatted
// body with the resolved names.
func rewriteMentions(body, formattedBody string, isHTML bool, resolve func(id.UserID) (string, bool)) string {
	if formattedBody == "" || !isHTML {
		return body
	}
	ctx := format.NewContext(context.Background())
	if body == "" {
		parser := newMentionParser(func(displayname string, mxid id.UserID) string {
			if name, ok := resolve(mxid); ok && name != "" {
				return name
			}
			return displayname
		})
		return strings.TrimSpace(parser.Parse(formattedBody, ctx))
	}
	var pills []mentionPill
	parser := newMentionParser(func(displayname string, mxid id.UserID) string {
		pills = append(pills, mentionPill{displayname, mxid})
		return displayname
	})
	parser.Parse(formattedBody, ctx)
	sort.SliceStable(pills, func(i, j int) bool { return len(pills[i].displayname) > len(pills[j].displayname) })
	seen := map[mentionPill]bool{}
	for _, p := range pills {
		if seen[p] {
			continue
		}
		seen[p] = true
		name, ok := resolve(p.mxid)
		if !ok || name == "" {
			continue
		}
		if p.displayname != "" && p.displayname != name {
			body = strings.ReplaceAll(body, p.displayname, name)
		}
		body = strings.ReplaceAll(body, string(p.mxid), name)
	}
	return body
}

// mentionResolver returns the resolver used by rewriteMentions for a portal: a bridged ghost user ID
// maps to the XMPP-side name (MUC occupant nick, own login localpart, roster name, Cheogram number,
// else the JID localpart); other user IDs are not resolved.
func (x *xmppClient) mentionResolver(ctx context.Context, portal *bridgev2.Portal) func(id.UserID) (string, bool) {
	parse := x.parseGhostHook
	if parse == nil {
		if portal == nil || portal.Bridge == nil || portal.Bridge.Matrix == nil {
			return func(id.UserID) (string, bool) { return "", false }
		}
		parse = portal.Bridge.Matrix.ParseGhostMXID
	}
	return func(mxid id.UserID) (string, bool) {
		ghostID, ok := parse(mxid)
		if !ok || ghostID == "" {
			return "", false
		}
		return x.xmppNameForGhost(networkid.UserID(ghostID)), true
	}
}

// xmppNameForGhost is the name an XMPP client would show for the ghost's JID.
func (x *xmppClient) xmppNameForGhost(ghostID networkid.UserID) string {
	j := string(ghostID)
	if strings.Contains(j, "/") {
		if nick := resourceOf(j); nick != "" {
			return nick
		}
		j = j[:strings.IndexByte(j, '/')]
	}
	local := j
	if i := strings.IndexByte(j, '@'); i >= 0 {
		local = j[:i]
	}
	if strings.EqualFold(j, string(x.loginID)) {
		return local
	}
	if name := x.lookupRosterName(j); name != "" {
		return name
	}
	if num, ok := cheogramE164(j); ok {
		return num
	}
	if local == "" {
		return j
	}
	return local
}
