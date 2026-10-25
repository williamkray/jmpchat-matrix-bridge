package connector

import (
	"encoding/xml"
	"html"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmpp/jid"
)

// Inbound mentions (XMPP -> Matrix): XEP-0372 references and, in rooms, nick heuristics become Matrix
// pills (formatted_body) plus m.mentions. Parsing (parseMentionRef) is memory-only and runs on the
// Serve goroutine; resolution and HTML building run in ConvertMessage (portal event loop). The body
// stays the plain text; without any mention the content is left exactly as it was.

const (
	referenceNS = "urn:xmpp:reference:0" // XEP-0372

	// maxMentionRefs caps the XEP-0372 mention references kept per message.
	maxMentionRefs = 20
	// maxMentionURILen caps the length of a reference uri attribute.
	maxMentionURILen = 512
	// maxMentionHeuristicRunes is the longest text the nick heuristics scan.
	maxMentionHeuristicRunes = 8192
	// maxLeadingNickRunes is the longest nick tried for the "nick: " addressing heuristic.
	maxLeadingNickRunes = 64
)

// mentionRef is one XEP-0372 type=mention reference: [begin, end) in Unicode code points of the body
// as sent, and the xmpp: uri of the mentioned entity.
type mentionRef struct {
	begin, end int
	uri        string
}

// mentionRange is a resolved mention: [begin, end) in code points of the Matrix text.
type mentionRange struct {
	begin, end int
	mxid       id.UserID
}

// parseMentionRef reads the attributes of a XEP-0372 <reference/>. Only type=mention with an xmpp:
// uri and 0 <= begin < end are accepted; anything else is "no usable reference".
func parseMentionRef(attrs []xml.Attr) (mentionRef, bool) {
	if attrValue(attrs, "type") != "mention" {
		return mentionRef{}, false
	}
	uri := attrValue(attrs, "uri")
	if uri == "" || len(uri) > maxMentionURILen || !strings.HasPrefix(uri, "xmpp:") {
		return mentionRef{}, false
	}
	begin, err1 := strconv.Atoi(attrValue(attrs, "begin"))
	end, err2 := strconv.Atoi(attrValue(attrs, "end"))
	if err1 != nil || err2 != nil || begin < 0 || end <= begin {
		return mentionRef{}, false
	}
	return mentionRef{begin: begin, end: end, uri: uri}, true
}

// shiftMentionRefs moves references by -lead code points (the blanks trimmed from the start of the
// body); references that would start before the trimmed body are dropped.
func shiftMentionRefs(refs []mentionRef, lead int) []mentionRef {
	if lead == 0 || len(refs) == 0 {
		return refs
	}
	out := refs[:0:0]
	for _, r := range refs {
		if r.begin-lead < 0 {
			continue
		}
		out = append(out, mentionRef{begin: r.begin - lead, end: r.end - lead, uri: r.uri})
	}
	return out
}

// mapMentionRefs maps references from the body as sent (orig) to the Matrix text. Unchanged text keeps
// them; a text produced by stripping the XEP-0428 fallback range shifts them (references inside the
// stripped range are dropped); any other rewrite (quote-line stripping) drops them all.
func mapMentionRefs(orig string, fb replyFallback, text string, refs []mentionRef) []mentionRef {
	if len(refs) == 0 || text == orig {
		return refs
	}
	if !fb.Valid || stripReplyFallback(orig, fb) != text {
		return nil
	}
	runes := []rune(orig)
	start, end := fb.Start, fb.End
	if start < 0 {
		start = 0
	}
	if start > len(runes) {
		start = len(runes)
	}
	if end > len(runes) {
		end = len(runes)
	}
	rest := string(runes[:start]) + string(runes[end:])
	lead := 0
	if start == 0 {
		for _, p := range []string{"\r", "\n"} {
			if strings.HasPrefix(rest, p) {
				rest = rest[len(p):]
				lead++
			}
		}
	}
	lead += len([]rune(rest)) - len([]rune(strings.TrimLeftFunc(rest, unicode.IsSpace)))
	removed := end - start
	var out []mentionRef
	for _, r := range refs {
		b, e := r.begin, r.end
		switch {
		case e <= start:
		case b >= end:
			b, e = b-removed, e-removed
		default:
			continue
		}
		b, e = b-lead, e-lead
		if b < 0 {
			continue
		}
		out = append(out, mentionRef{begin: b, end: e, uri: r.uri})
	}
	return out
}

// isMentionWordRune reports whether r continues a word (letter or digit).
func isMentionWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }

// buildInboundMentions turns resolved references (refs, already mapped to MXIDs) or, when none are
// usable, the nick heuristics into an HTML formatted body and the mentioned user IDs. refs outside the
// text, empty or overlapping are dropped. Heuristics: the leading "nick: " / "nick, " addressing of a
// seen occupant (leadingNick resolves an exact nick) and every whole-word, case-insensitive occurrence
// of ownNick (pill to ownMXID; skipped when ownNick or ownMXID is empty). It returns "" and nil when
// nothing is mentioned; the caller keeps the content unchanged then.
func buildInboundMentions(text string, refs []mentionRange, ownNick string, ownMXID id.UserID, leadingNick func(nick string) (id.UserID, bool)) (string, []id.UserID) {
	runes := []rune(text)
	ranges := validMentionRanges(refs, len(runes))
	if len(ranges) == 0 && len(runes) <= maxMentionHeuristicRunes {
		if r, ok := leadingNickRange(runes, leadingNick); ok {
			ranges = append(ranges, r)
		}
		if ownNick != "" && ownMXID != "" {
			for _, r := range ownNickRanges(runes, ownNick, ownMXID) {
				if !overlapsAny(ranges, r) {
					ranges = append(ranges, r)
				}
			}
		}
		sort.Slice(ranges, func(i, j int) bool { return ranges[i].begin < ranges[j].begin })
	}
	if len(ranges) == 0 {
		return "", nil
	}
	var sb strings.Builder
	var users []id.UserID
	seen := map[id.UserID]bool{}
	pos := 0
	for _, r := range ranges {
		sb.WriteString(escapeMentionText(string(runes[pos:r.begin])))
		sb.WriteString(`<a href="https://matrix.to/#/` + html.EscapeString(string(r.mxid)) + `">`)
		sb.WriteString(escapeMentionText(string(runes[r.begin:r.end])))
		sb.WriteString("</a>")
		pos = r.end
		if !seen[r.mxid] {
			seen[r.mxid] = true
			users = append(users, r.mxid)
		}
	}
	sb.WriteString(escapeMentionText(string(runes[pos:])))
	return sb.String(), users
}

// escapeMentionText HTML-escapes text and turns newlines into <br>.
func escapeMentionText(s string) string {
	s = html.EscapeString(s)
	s = strings.ReplaceAll(s, "\r\n", "\n")
	return strings.ReplaceAll(s, "\n", "<br>")
}

// validMentionRanges keeps the ranges that lie inside n code points, are non-empty, have a target and do
// not overlap an earlier (by begin) accepted range; the result is sorted by begin.
func validMentionRanges(refs []mentionRange, n int) []mentionRange {
	sorted := make([]mentionRange, 0, len(refs))
	for _, r := range refs {
		if r.mxid != "" && r.begin >= 0 && r.end > r.begin && r.end <= n {
			sorted = append(sorted, r)
		}
	}
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].begin < sorted[j].begin })
	var out []mentionRange
	for _, r := range sorted {
		if len(out) > 0 && r.begin < out[len(out)-1].end {
			continue
		}
		out = append(out, r)
	}
	return out
}

// overlapsAny reports whether r overlaps one of ranges.
func overlapsAny(ranges []mentionRange, r mentionRange) bool {
	for _, o := range ranges {
		if r.begin < o.end && o.begin < r.end {
			return true
		}
	}
	return false
}

// leadingNickRange finds "<nick>: " / "<nick>, " (or the separator at the end of the text) at the start
// of the text where leadingNick knows the exact nick; the longest known nick wins. The range covers the
// nick only.
func leadingNickRange(runes []rune, leadingNick func(string) (id.UserID, bool)) (mentionRange, bool) {
	if leadingNick == nil {
		return mentionRange{}, false
	}
	var best mentionRange
	found := false
	for i := 1; i < len(runes) && i <= maxLeadingNickRunes; i++ {
		if runes[i] != ':' && runes[i] != ',' {
			continue
		}
		if i+1 < len(runes) && runes[i+1] != ' ' {
			continue
		}
		if mxid, ok := leadingNick(string(runes[:i])); ok && mxid != "" {
			best, found = mentionRange{begin: 0, end: i, mxid: mxid}, true
		}
	}
	return best, found
}

// ownNickRanges returns every whole-word, case-insensitive occurrence of nick in runes. A word boundary
// is the text edge or a rune that is not a letter or digit; the whole nick string is matched.
func ownNickRanges(runes []rune, nick string, mxid id.UserID) []mentionRange {
	n := len([]rune(nick))
	var out []mentionRange
	for i := 0; i+n <= len(runes); i++ {
		if i > 0 && isMentionWordRune(runes[i-1]) {
			continue
		}
		if i+n < len(runes) && isMentionWordRune(runes[i+n]) {
			continue
		}
		if strings.EqualFold(string(runes[i:i+n]), nick) {
			out = append(out, mentionRange{begin: i, end: i + n, mxid: mxid})
			i += n - 1
		}
	}
	return out
}

// ghostMXID returns the Matrix user ID of the ghost for a network user id, "" when unknown (nil-safe).
func (x *xmppClient) ghostMXID(portal *bridgev2.Portal, uid networkid.UserID) id.UserID {
	if x.formatGhostHook != nil {
		return x.formatGhostHook(uid)
	}
	if portal == nil || portal.Bridge == nil || portal.Bridge.Matrix == nil {
		return ""
	}
	return portal.Bridge.Matrix.FormatGhostMXID(uid)
}

// ownMXID is the Matrix user of this login ("" when unknown).
func (x *xmppClient) ownMXID() id.UserID {
	if x.login == nil || x.login.UserLogin == nil {
		return ""
	}
	return x.login.UserMXID
}

// applyInboundMentions adds Matrix mentions to a text content built from the XMPP body orig: refs are
// the XEP-0372 references of orig (fb is its reply fallback range), fromMe says the message was sent by
// the bridged user (never self-mentioned), isMUC enables occupant mentions and the nick heuristics. In
// a one-to-one chat only a reference to our own JID is honoured. Content without a mention is left
// untouched (no Format, FormattedBody or Mentions).
func (x *xmppClient) applyInboundMentions(portal *bridgev2.Portal, content *event.MessageEventContent, orig string, fb replyFallback, refs []mentionRef, fromMe, isMUC bool) {
	if portal == nil || content == nil || content.Body == "" {
		return
	}
	room := string(portal.ID)
	ownOcc := ""
	ownNick := ""
	if isMUC {
		ownOcc = x.getMUCOurOccupantJID(room)
		ownNick = resourceOf(ownOcc)
	}
	ownMXID := id.UserID("")
	if !fromMe {
		ownMXID = x.ownMXID()
	}
	var resolved []mentionRange
	for _, r := range mapMentionRefs(orig, fb, content.Body, refs) {
		if mxid := x.resolveMentionURI(portal, r.uri, room, ownOcc, ownMXID, fromMe, isMUC); mxid != "" {
			resolved = append(resolved, mentionRange{begin: r.begin, end: r.end, mxid: mxid})
		}
	}
	var leading func(string) (id.UserID, bool)
	if isMUC {
		leading = func(nick string) (id.UserID, bool) {
			if nick == ownNick {
				return "", false // own nick is the whole-word heuristic's job
			}
			full := room + "/" + nick
			if !x.mucOccupantSeen(full) {
				return "", false
			}
			mxid := x.ghostMXID(portal, networkid.UserID(full))
			return mxid, mxid != ""
		}
	} else {
		ownNick = ""
	}
	formatted, users := buildInboundMentions(content.Body, resolved, ownNick, ownMXID, leading)
	if len(users) == 0 {
		return
	}
	content.Format = event.FormatHTML
	content.FormattedBody = formatted
	content.Mentions = &event.Mentions{UserIDs: users}
}

// resolveMentionURI maps a reference uri to a Matrix user: our own occupant JID or bare login JID is
// the bridged user (ownMXID; nothing when the sender is the bridged user), another occupant of this
// room that spoke this session is its ghost; everything else is "". Memory only.
func (x *xmppClient) resolveMentionURI(portal *bridgev2.Portal, uri, room, ownOcc string, ownMXID id.UserID, fromMe, isMUC bool) id.UserID {
	raw := strings.TrimPrefix(uri, "xmpp:")
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i]
	}
	raw, err := url.PathUnescape(raw)
	if err != nil {
		return ""
	}
	j, err := jid.Parse(raw)
	if err != nil {
		return ""
	}
	full := j.String()
	if (ownOcc != "" && strings.EqualFold(full, ownOcc)) || strings.EqualFold(full, string(x.loginID)) {
		if fromMe {
			return ""
		}
		return ownMXID
	}
	if !isMUC || j.Resourcepart() == "" || !strings.EqualFold(j.Bare().String(), room) || !x.mucOccupantSeen(full) {
		return ""
	}
	return x.ghostMXID(portal, networkid.UserID(full))
}
