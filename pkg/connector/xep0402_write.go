package connector

import (
	"context"
	"encoding/xml"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"mellium.im/xmpp/bookmarks"
	"mellium.im/xmpp/pubsub"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// XEP-0402 writes. Only the explicit join command (after the join is confirmed) and the leave command
// write; auto-rejoin, imports, events, sending and viewing never do (except the one-time backfill of
// xep0402_backfill.go). Every write is a read-modify-write: a fresh fetch of the node, mergeBookmark on
// the server's item (name, password and extensions of other clients survive), then a publish WITH
// publish-options. A failed publish is never retried without options (a node with a non-private access
// model would expose the room list and passwords).

const (
	// bookmarkWriteTimeout caps one fetch+publish.
	bookmarkWriteTimeout = 20 * time.Second
	// bookmarkFormNS is XEP-0004 data forms.
	bookmarkFormNS = "jabber:x:data"
	// publishOptionsFormType is the FORM_TYPE of the publish-options form.
	publishOptionsFormType = "http://jabber.org/protocol/pubsub#publish-options"
)

// bookmarkWriteOutcome is what publishBookmarkFor did.
type bookmarkWriteOutcome int

const (
	bmSkipped bookmarkWriteOutcome = iota // gate off, writes disabled, or not connected
	bmNoop                                // nothing to change
	bmWritten                             // published
	bmFailed                              // fetch or publish failed
)

// bookmarkPublishTokens builds the pubsub publish payload of one bookmark: the item under its stored id
// and the XEP-0402 / Prosody mod_bookmarks node defaults as publish-options.
func bookmarkPublishTokens(c bookmarkConf) []xml.Token {
	el := func(space, local string, attrs ...xml.Attr) xml.StartElement {
		return xml.StartElement{Name: xml.Name{Space: space, Local: local}, Attr: attrs}
	}
	attr := func(k, v string) xml.Attr { return xml.Attr{Name: xml.Name{Local: k}, Value: v} }
	toks := []xml.Token{
		el(pubsub.NS, "pubsub"),
		el("", "publish", attr("node", bookmarks.NS)),
		el("", "item", attr("id", c.ID)),
	}
	toks = append(toks, c.tokens()...)
	toks = append(toks, xml.EndElement{Name: xml.Name{Local: "item"}}, xml.EndElement{Name: xml.Name{Local: "publish"}})
	toks = append(toks, el("", "publish-options"), el(bookmarkFormNS, "x", attr("type", "submit")))
	field := func(name, typ, value string) {
		attrs := []xml.Attr{attr("var", name)}
		if typ != "" {
			attrs = append(attrs, attr("type", typ))
		}
		toks = append(toks,
			el("", "field", attrs...),
			el("", "value"), xml.CharData(value), xml.EndElement{Name: xml.Name{Local: "value"}},
			xml.EndElement{Name: xml.Name{Local: "field"}})
	}
	field("FORM_TYPE", "hidden", publishOptionsFormType)
	field("pubsub#persist_items", "", "true")
	field("pubsub#max_items", "", "max")
	field("pubsub#send_last_published_item", "", "never")
	field("pubsub#access_model", "", "whitelist")
	toks = append(toks,
		xml.EndElement{Name: xml.Name{Space: bookmarkFormNS, Local: "x"}},
		xml.EndElement{Name: xml.Name{Local: "publish-options"}},
		xml.EndElement{Name: xml.Name{Space: pubsub.NS, Local: "pubsub"}})
	return toks
}

// publishErrorDisablesWrites classifies a publish error: an IQ error answer that means "this server or
// node will not take private writes" (precondition not met / unsupported publish-options / forbidden)
// disables writes for the session. Network errors and timeouts do not.
func publishErrorDisablesWrites(err error) bool {
	var se stanza.Error
	if !errors.As(err, &se) {
		return false
	}
	switch se.Condition {
	case stanza.Conflict, stanza.FeatureNotImplemented, stanza.BadRequest, stanza.Forbidden, stanza.NotAllowed:
		return true
	}
	return false
}

// publishBookmarkIQ sends the publish. The hook replaces the IQ in tests.
func (x *xmppClient) publishBookmarkIQ(ctx context.Context, c bookmarkConf) error {
	if x.bm.publishHook != nil {
		return x.bm.publishHook(ctx, c)
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return errBookmarksNotConnected
	}
	iq := stanza.IQ{Type: stanza.SetIQ, ID: newStanzaID()}
	return sess.UnmarshalIQElement(ctx, &sliceTokenReader{toks: bookmarkPublishTokens(c)}, iq, nil)
}

// bookmarkStoredFor returns the item of a room from a fetched list.
func bookmarkStoredFor(bms []bookmarkConf, key string) *bookmarkConf {
	for i := range bms {
		if bookmarkKey(bms[i].ID) == key {
			c := bms[i]
			return &c
		}
	}
	return nil
}

// publishBookmarkFor sets autojoin (and, per the nick rule of mergeBookmark, the nick) of a room's
// bookmark. create says whether a missing bookmark is created (join) or left alone (leave). Never call
// it on the Serve goroutine. Failures are logged and returned as an outcome, never as an error: a
// bookmark problem must not fail a join or leave.
func (x *xmppClient) publishBookmarkFor(ctx context.Context, room string, autojoin bool, nick string, create bool) bookmarkWriteOutcome {
	key := bookmarkKey(room)
	log := x.logFor(ctx)
	s, ok := x.ensureBookmarkGate(ctx)
	if !ok || !s.Write || x.bm.writesDisabled() {
		return bmSkipped
	}
	x.bm.writeMu.Lock()
	defer x.bm.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, bookmarkWriteTimeout)
	defer cancel()

	// Cache pre-check: when the node was fetched this session and nothing would change, skip every IQ.
	if cached, hit, fetched := x.bm.lookup(key); fetched {
		if hit {
			if _, changed := mergeBookmark(&cached, key, autojoin, nick); !changed {
				return bmNoop
			}
		} else if !create {
			return bmNoop
		}
	}
	// The cache is never the base of a write: a fresh fetch is.
	bms, info, err := x.bookmarkFetch(ctx)
	if err != nil {
		log.Warn().Err(err).Str("room", key).Msg("Bookmarks: fetch before write failed; not writing")
		return bmFailed
	}
	x.bm.setCache(bms)
	existing := bookmarkStoredFor(bms, key)
	if existing == nil && !create {
		return bmNoop
	}
	if existing == nil && info.mayHide(key) {
		// The item may exist but was undecodable or cut off: creating one would overwrite it.
		log.Warn().Str("room", key).Msg("Bookmarks: the room's bookmark could not be read; not writing")
		return bmFailed
	}
	merged, changed := mergeBookmark(existing, key, autojoin, nick)
	if !changed {
		return bmNoop
	}
	ictx, span := telemetry.StartChild(ctx, "xmpp.iq.bookmarks_publish", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Bool("xmpp.bookmarks.autojoin", autojoin), attribute.Bool("xmpp.bookmarks.created", existing == nil))
	}
	// Cache first, so the PEP echo of this publish (which can arrive before the IQ result) compares
	// equal and is ignored by the event worker; restored when the publish fails.
	x.bm.cacheSet(key, merged)
	if err := x.publishBookmarkIQ(ictx, merged); err != nil {
		x.bm.cacheRestore(key, existing)
		telemetry.Fail(span, err)
		if publishErrorDisablesWrites(err) {
			if x.bm.disableWrites() {
				log.Warn().Err(err).Msg("Bookmarks: the server refused a private bookmark write (non-private node configuration or no publish-options); bookmark writes are disabled for this session")
			}
		} else {
			log.Warn().Err(err).Str("room", key).Msg("Bookmarks: publish failed")
		}
		return bmFailed
	}
	log.Info().Str("room", key).Bool("autojoin", autojoin).Bool("created", existing == nil).Msg("Bookmarks: published")
	return bmWritten
}

// bookmarkAfterJoin runs in its own goroutine (started by handlePresence when the explicit join command's
// join is confirmed): the room's bookmark gets autojoin=true. The nick is only offered when the join
// succeeded with the preferred nick: a 409 alternative stays bridge-local (portal metadata), so other
// clients keep their own nick.
func (x *xmppClient) bookmarkAfterJoin(room, base, ourNick string, conflicted bool) {
	ctx, span := telemetry.Start(context.Background(), "xmpp.bookmarks.after_join", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	nick := ourNick
	if conflicted && ourNick != base {
		nick = ""
	}
	x.publishJoinBookmark(ctx, room, nick)
}

// publishJoinBookmark creates/enables the room's bookmark (autojoin on, nick only when offered).
func (x *xmppClient) publishJoinBookmark(ctx context.Context, room, nick string) {
	if x.joinBookmarkHook != nil {
		x.joinBookmarkHook(room, nick)
		return
	}
	x.publishBookmarkFor(ctx, room, true, nick, true)
}

// bookmarkAfterLeave sets autojoin=false after an explicit leave and keeps the bookmark (never retracts,
// never creates one). It runs in the caller's (command) goroutine.
func (x *xmppClient) bookmarkAfterLeave(ctx context.Context, room string) {
	x.publishBookmarkFor(ctx, room, false, "", false)
}
