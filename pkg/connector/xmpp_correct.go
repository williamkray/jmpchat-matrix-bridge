package connector

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Inbound XEP-0308 message corrections become a simplevent.Message of type RemoteEventEdit whose
// pre-handler resolves and validates the target in the portal event loop (the same shape as
// retractions, see xmpp_retract_event.go). Nothing here runs on the Serve goroutine except building the
// event. A correction never creates a portal and is never bridged as a new message: when its target is
// unknown, foreign, retracted or media, it is ignored.

const correctedKeyPrefix = "jabber_corrected:"

// correction is one inbound correction stanza with everything validation and conversion need.
type correction struct {
	claim         senderClaim
	target        string // <replace id>
	corrID        string // the correction stanza's id (or origin-id); "" when not deduplicable
	body          string
	replyID       string
	replyFallback replyFallback
	mentions      []mentionRef // XEP-0372 mention references (offsets in body code points)
	hasOOB        bool
	encrypted     bool   // the correction payload is encrypted: the body is only a fallback text, never applied
	senderID      string // sender id as stored on rows (bare JID / room/nick / login id)
	ts            time.Time
}

func (c correction) occupantID() string { return c.claim.occupantID }

// refKind picks the lookup order for the <replace id> target.
func (c correction) refKind() refKind {
	if c.claim.groupchat {
		return refCorrectionMUC
	}
	return refCorrectionDM
}

// correctedKey is the KV key that records an applied correction: it maps the correction stanza's id
// (ref), under the sender's scope, to the bridgev2 message ID of the ORIGINAL it corrected. It serves
// two purposes: a replay (MUC history, overlapping MAM windows, the archive copy of a correction the
// bridge itself sent) finds the id and is dropped, and a client that corrects the previous correction
// instead of the original (<replace id> naming that correction) resolves back to the original. Format:
// jabber_corrected:<login id>:<lower-cased portal id>:<scope>:<ref>, never garbage collected.
func correctedKey(login networkid.UserLoginID, portal networkid.PortalID, scope, ref string) database.Key {
	return database.Key(correctedKeyPrefix + string(login) + ":" + strings.ToLower(string(portal)) + ":" + scope + ":" + ref)
}

// pickCorrectionTarget returns the first candidate (lookup order) that the sender owns, or the first
// rejection reason ("not_found" when there are no candidates).
func pickCorrectionTarget(c correction, p portalFacts, cands []*database.Message) (*database.Message, string) {
	reason := "not_found"
	for i, cand := range cands {
		ok, why := validateOwnership(c.claim, p, cand)
		if ok {
			return cand, ""
		}
		if i == 0 {
			reason = why
		}
	}
	return nil, reason
}

// correctionEnv is the seam between the pre/post handlers and bridgev2/the XMPP client, so the handlers
// can be tested without a bridge.
type correctionEnv interface {
	findByRef(ctx context.Context, key networkid.PortalKey, ref string, kind refKind) ([]*database.Message, error)
	facts(ctx context.Context, portal *bridgev2.Portal) portalFacts
	kvGet(ctx context.Context, key database.Key) string
	kvSet(ctx context.Context, key database.Key, val string)
	isRetracted(ctx context.Context, portal networkid.PortalID, who retractedSender, refs ...string) bool
	// bodyText is the Matrix text for the corrected body (reply fallback handling).
	bodyText(ctx context.Context, portal *bridgev2.Portal, c correction) string
	// applyMentions adds Matrix mentions (formatted body, m.mentions) for the corrected text to content.
	applyMentions(portal *bridgev2.Portal, content *event.MessageEventContent, c correction)
	log(ctx context.Context) *zerolog.Logger
}

// correctionState is shared between the pre, convert and post handler of one event.
type correctionState struct {
	target    *database.Message // validated original (nil until the pre-handler resolved it)
	converted bool
	hctx      context.Context // ctx carrying the handling span started by the pre-handler (nil if it did not run)
}

// newCorrectionEvent builds the remote event for a correction. Its target is the raw <replace id>
// until the pre-handler swaps in the bridgev2 message ID. CreatePortal is false.
func newCorrectionEvent(env correctionEnv, key networkid.PortalKey, sender bridgev2.EventSender, c correction) *simplevent.Message[*correction] {
	st := &correctionState{}
	ts := c.ts
	if ts.IsZero() {
		ts = time.Now()
	}
	evt := &simplevent.Message[*correction]{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventEdit,
			PortalKey: key,
			Sender:    sender,
			Timestamp: ts,
			LogContext: func(zc zerolog.Context) zerolog.Context {
				return zc.Str("correction_target", c.target).Str("correction_id", c.corrID)
			},
		},
		TargetMessage: networkid.MessageID(c.target),
		Data:          &c,
	}
	evt.ConvertEditFunc = func(ctx context.Context, portal *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message, data *correction) (*bridgev2.ConvertedEdit, error) {
		return convertCorrection(ctx, env, st, portal, existing, *data)
	}
	evt.PreHandleWithResultFunc = func(ctx context.Context, portal *bridgev2.Portal) *bridgev2.EventHandlingResult {
		return preHandleCorrection(ctx, env, evt, st, c, portal)
	}
	evt.PostHandleFunc = func(ctx context.Context, portal *bridgev2.Portal) {
		if st.hctx != nil {
			ctx = st.hctx
		}
		postHandleCorrection(ctx, env, st, key, c)
		telemetry.EndRemoteEvent(ctx)
	}
	return evt
}

// correctionAlreadyApplied reports whether this correction stanza id was recorded for the sender.
func correctionAlreadyApplied(ctx context.Context, env correctionEnv, login networkid.UserLoginID, portal networkid.PortalID, c correction) bool {
	if c.corrID == "" {
		return false
	}
	for _, sc := range senderScopes(c.senderID, c.occupantID()) {
		if env.kvGet(ctx, correctedKey(login, portal, sc, c.corrID)) != "" {
			return true
		}
	}
	return false
}

// preHandleCorrection resolves and validates the target. On success it sets evt.TargetMessage to the
// validated message ID and marks the sender ForceEditOrigSender: ownership is already proven here, and
// bridgev2 then uses the original sender's intent (so a correction of a Matrix-originated message
// by the user works, instead of hitting bridgev2's "sender doesn't match" ignore). It returns nil to let
// bridgev2 apply the edit, or Ignored/Failed when the event is finished here (bridgev2 then calls no
// post-handler, so the handling span ends here).
func preHandleCorrection(ctx context.Context, env correctionEnv, evt *simplevent.Message[*correction], st *correctionState, c correction, portal *bridgev2.Portal) *bridgev2.EventHandlingResult {
	// The handling span starts here, not in MutateContext (see preHandleRetraction).
	ctx = startLazyRemoteEvent(ctx)
	st.hctx = ctx
	rctx, span := telemetry.StartChild(ctx, "bridge.resolve_edit_target")
	defer span.End()
	log := env.log(rctx)
	ignore := func(reason string) *bridgev2.EventHandlingResult {
		span.SetAttributes(attribute.String("bridge.rejected_reason", reason))
		log.Debug().Str("reason", reason).Msg("Ignoring correction")
		telemetry.EndRemoteEvent(ctx)
		res := bridgev2.EventHandlingResultIgnored
		return &res
	}
	span.SetAttributes(attribute.String("bridge.correction.channel", c.claim.channel))

	if strings.TrimSpace(c.body) == "" {
		return ignore("no_body")
	}
	key := evt.GetPortalKey()
	pf := env.facts(rctx, portal)
	if correctionAlreadyApplied(rctx, env, pf.loginID, key.ID, c) {
		return ignore("replayed_correction")
	}
	cands, err := env.findByRef(rctx, key, c.target, c.refKind())
	if err != nil {
		telemetry.Fail(span, err)
		log.Err(err).Msg("Failed to look up correction target")
		telemetry.EndRemoteEvent(ctx)
		res := bridgev2.EventHandlingResultFailed.WithError(err)
		return &res
	}
	if len(cands) == 0 {
		// An older client corrects its previous correction: resolve through the alias record.
		for _, sc := range senderScopes(c.senderID, c.occupantID()) {
			origID := env.kvGet(rctx, correctedKey(pf.loginID, key.ID, sc, c.target))
			if origID == "" {
				continue
			}
			if cands, err = env.findByRef(rctx, key, origID, c.refKind()); err != nil {
				telemetry.Fail(span, err)
				telemetry.EndRemoteEvent(ctx)
				res := bridgev2.EventHandlingResultFailed.WithError(err)
				return &res
			}
			span.SetAttributes(attribute.Bool("bridge.correction.alias", true))
			break
		}
	}
	target, reason := pickCorrectionTarget(c, pf, cands)
	span.SetAttributes(attribute.Bool("bridge.found", target != nil))
	if target == nil {
		return ignore(reason)
	}
	meta := targetMeta(target)
	who := retractedSender{senderID: string(target.SenderID)}
	refs := []string{string(target.ID)}
	if meta != nil {
		who.occupantID, who.stanzaID = meta.OccupantID, meta.StanzaID
		refs = append(refs, meta.XMPPMessageID, meta.StanzaID, meta.OriginID)
	}
	if env.isRetracted(rctx, key.ID, who, refs...) {
		return ignore("target_retracted")
	}
	if c.hasOOB || (meta != nil && meta.Media) {
		return ignore("media_correction_unsupported")
	}
	evt.TargetMessage = target.ID
	evt.Sender.ForceEditOrigSender = true
	st.target = target
	return nil
}

// convertCorrection builds the Matrix edit: new text for the first part of the original.
func convertCorrection(ctx context.Context, env correctionEnv, st *correctionState, portal *bridgev2.Portal, existing []*database.Message, c correction) (*bridgev2.ConvertedEdit, error) {
	if len(existing) == 0 {
		return nil, bridgev2.ErrIgnoringRemoteEvent
	}
	text := env.bodyText(ctx, portal, c)
	if strings.TrimSpace(text) == "" {
		return nil, bridgev2.ErrIgnoringRemoteEvent
	}
	st.converted = true
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: text}
	env.applyMentions(portal, content, c)
	return &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{{
		Part:    existing[0],
		Type:    event.EventMessage,
		Content: content,
	}}}, nil
}

// postHandleCorrection records the applied correction (replay dedupe + alias for chained corrections).
func postHandleCorrection(ctx context.Context, env correctionEnv, st *correctionState, key networkid.PortalKey, c correction) {
	if !st.converted || st.target == nil || c.corrID == "" {
		return
	}
	login := env.facts(ctx, nil).loginID
	for _, sc := range senderScopes(c.senderID, c.occupantID()) {
		env.kvSet(ctx, correctedKey(login, key.ID, sc, c.corrID), string(st.target.ID))
	}
	st.converted = false
}

// ---- real environment ----

type xmppCorrectionEnv struct{ x *xmppClient }

func (e xmppCorrectionEnv) findByRef(ctx context.Context, key networkid.PortalKey, ref string, kind refKind) ([]*database.Message, error) {
	return findMessagesByXMPPRef(ctx, e.x.conn.br.DB, key, ref, kind)
}

func (e xmppCorrectionEnv) facts(ctx context.Context, portal *bridgev2.Portal) portalFacts {
	if portal == nil {
		return portalFacts{loginID: e.x.loginID}
	}
	return xmppRetractionEnv{e.x}.facts(ctx, portal)
}

func (e xmppCorrectionEnv) kvGet(ctx context.Context, key database.Key) string {
	if kv := e.x.kv(); kv != nil {
		return kv.Get(ctx, key)
	}
	return ""
}

func (e xmppCorrectionEnv) kvSet(ctx context.Context, key database.Key, val string) {
	if kv := e.x.kv(); kv != nil {
		kv.Set(ctx, key, val)
	}
}

func (e xmppCorrectionEnv) isRetracted(ctx context.Context, portal networkid.PortalID, who retractedSender, refs ...string) bool {
	return e.x.isRetracted(ctx, portal, who, refs...)
}

func (e xmppCorrectionEnv) bodyText(ctx context.Context, portal *bridgev2.Portal, c correction) string {
	key := portal.PortalKey
	text, _ := textForBody(ctx, e.x, key, refIsMUCFor(c.claim.groupchat, portal, key), c.body, c.replyID, c.replyFallback)
	return text
}

func (e xmppCorrectionEnv) applyMentions(portal *bridgev2.Portal, content *event.MessageEventContent, c correction) {
	key := portal.PortalKey
	// A corrected message is "ours" when the stored sender id is our login (own echo/carbon).
	fromMe := c.senderID == string(e.x.loginID)
	e.x.applyInboundMentions(portal, content, c.body, c.replyFallback, c.mentions, fromMe, refIsMUCFor(c.claim.groupchat, portal, key))
}

func (e xmppCorrectionEnv) log(ctx context.Context) *zerolog.Logger { return e.x.logFor(ctx) }

// ---- queueing ----

// queueCorrection queues the correction event through queueRemoteEvent (span context + handling span).
func (x *xmppClient) queueCorrection(ctx context.Context, key networkid.PortalKey, sender bridgev2.EventSender, c correction) bridgev2.EventHandlingResult {
	return x.queueRemoteEvent(ctx, newCorrectionEvent(xmppCorrectionEnv{x}, key, sender, c))
}

// handleInboundCorrection is the live/carbon entry point (Serve goroutine: span, debug line and queueing
// only; no DB or IQ work). It logs ids, the channel and lengths, never the corrected text.
func (x *xmppClient) handleInboundCorrection(c correction, key networkid.PortalKey, sender bridgev2.EventSender) {
	if c.encrypted {
		// Editing with the sender's fallback text ("I sent you an OMEMO encrypted message...") would
		// overwrite the original with noise; the plaintext is not readable here.
		x.logFor(context.Background()).Debug().Str("reason", "encrypted").Str("channel", c.claim.channel).Msg("Ignoring encrypted message correction")
		return
	}
	ctx, span := startReceiveSpan("correction", c.claim.msgType, c.claim.fromFull, string(key.ID), c.corrID, c.hasOOB, c.replyID != "", false)
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Bool("xmpp.correction", true), attribute.String("xmpp.correction.channel", c.claim.channel))
	}
	x.logFor(ctx).Debug().
		Str("channel", c.claim.channel).
		Str("target", c.target).
		Str("id", c.corrID).
		Int("body_len", len(c.body)).
		Bool("has_oob", c.hasOOB).
		Bool("occupant_id_present", c.occupantID() != "").
		Msg("Inbound message correction")
	x.queueCorrection(ctx, key, sender, c)
}

// queueMAMCorrection queues the correction event of an archived correction item (validated like a live
// one) and reports whether the item was a correction (such an item is never bridged as a message, even
// when it has no body). It never registers the portal for catch-up or triggers a backfill.
func (x *xmppClient) queueMAMCorrection(ctx context.Context, key networkid.PortalKey, item mamResultItem) bool {
	if item.Extras.Replace == "" || item.Retract != nil {
		return false
	}
	if item.Body == "" {
		return true
	}
	if item.Extras.Encryption != "" {
		x.logFor(ctx).Debug().Str("reason", "encrypted").Str("channel", retractChanMAM).Msg("Ignoring encrypted message correction")
		return true
	}
	corrID := mamCorrectionID(item)
	ts := item.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	c := correction{
		claim:    senderClaim{channel: retractChanMAM, msgType: item.Type, fromFull: item.From},
		target:   item.Extras.Replace,
		corrID:   corrID,
		body:     item.Body,
		senderID: bareJID(item.From),
		ts:       ts,
	}
	x.queueCorrection(ctx, key, x.makeEventSender(bareJID(item.From)), c)
	return true
}

// mamCorrectionID is the dedupe id of an archived correction, the same value the live path uses
// (firstNonEmpty(id, origin-id)): a generated "xmp:" stanza id is not stable across fetches and does not
// count, so a correction that only carries an origin-id still dedupes and live and MAM keys match.
func mamCorrectionID(item mamResultItem) string {
	id := item.StanzaID
	if strings.HasPrefix(id, "xmp:") {
		id = ""
	}
	return firstNonEmpty(id, item.OriginID)
}

// foldedCorrection is one correction applied to an original by foldBatchCorrectionsDetailed.
type foldedCorrection struct {
	corr, orig mamResultItem
}

// foldBatchCorrections is foldBatchCorrectionsDetailed without the applied list.
func foldBatchCorrections(items []mamResultItem) []mamResultItem {
	kept, _ := foldBatchCorrectionsDetailed(items)
	return kept
}

// foldBatchCorrectionsDetailed applies the corrections found in a fetched MAM batch to their originals
// in the same batch (FetchMessages returns BackfillMessages, which cannot carry edits) and drops every
// correction item. A correction applies to the item whose id or origin-id it names, sent by the same
// lower-cased bare JID; a later correction naming an earlier correction's id follows the chain, and the
// newest one wins. Corrections whose original is not in the batch, and corrections from a Cheogram
// group MMS JID (the shared from cannot identify the sender; the live path rejects them too) are
// dropped (the original is backfilled in its old form: documented gap). The kept items keep the input
// order. MAM result order is implementation-defined (see FetchMessages), so nothing relies on it:
// corrections are resolved in rounds, a correction becoming applicable only once its target (an
// original or an already applied correction) is known, so a chain applies in chain order whatever the
// timestamps say; corrections applicable in the same round go oldest first (input order on ties).
// The applied corrections are returned so the caller can record their dedupe keys.
func foldBatchCorrectionsDetailed(items []mamResultItem) ([]mamResultItem, []foldedCorrection) {
	var pending []int
	for i, it := range items {
		if it.Extras.Replace != "" && it.Retract == nil {
			pending = append(pending, i)
		}
	}
	if len(pending) == 0 {
		return items, nil
	}
	isCorr := make(map[int]bool, len(pending))
	for _, i := range pending {
		isCorr[i] = true
	}
	// Oldest first, input order for equal (or missing) timestamps.
	sort.SliceStable(pending, func(a, b int) bool { return items[pending[a]].Timestamp.Before(items[pending[b]].Timestamp) })

	type ref struct{ from, id string }
	byRef := map[ref]int{} // id or origin-id of an original (and of an applied correction) -> original index
	for i, it := range items {
		if isCorr[i] {
			continue
		}
		from := strings.ToLower(bareJID(it.From))
		for _, id := range []string{it.StanzaID, it.OriginID} {
			if id != "" {
				byRef[ref{from, id}] = i
			}
		}
	}
	out := make([]mamResultItem, len(items))
	copy(out, items)
	var folded []foldedCorrection
	for len(pending) > 0 {
		var next []int
		progress := false
		for _, ci := range pending {
			c := items[ci]
			if c.Body == "" || c.Extras.Encryption != "" {
				continue // encrypted: the body is a fallback text, never folded into the original
			}
			if _, mms := ParseCheogramGroupMMSJID(bareJID(c.From)); mms {
				continue
			}
			from := strings.ToLower(bareJID(c.From))
			oi, ok := byRef[ref{from, c.Extras.Replace}]
			if !ok {
				next = append(next, ci) // maybe the target is a correction applied later
				continue
			}
			progress = true
			out[oi].Body = c.Body
			folded = append(folded, foldedCorrection{corr: c, orig: items[oi]})
			for _, id := range []string{c.StanzaID, c.OriginID} {
				if id != "" {
					byRef[ref{from, id}] = oi
				}
			}
		}
		if !progress {
			break
		}
		pending = next
	}
	kept := make([]mamResultItem, 0, len(items)-len(isCorr))
	for i, it := range out {
		if !isCorr[i] {
			kept = append(kept, it)
		}
	}
	return kept, folded
}

// foldedWithin keeps the folded corrections whose original is one of items.
func foldedWithin(folded []foldedCorrection, items []mamResultItem) []foldedCorrection {
	var out []foldedCorrection
	for _, f := range folded {
		for _, it := range items {
			if it.StanzaID == f.orig.StanzaID && it.OriginID == f.orig.OriginID && it.From == f.orig.From && it.Timestamp.Equal(f.orig.Timestamp) {
				out = append(out, f)
				break
			}
		}
	}
	return out
}

// recordFoldedCorrections stores the dedupe keys of corrections that were folded into their originals
// (same key and value shape as postHandleCorrection), so a later catch-up that sees the same archived
// correction drops it instead of editing the freshly backfilled message a second time.
func (x *xmppClient) recordFoldedCorrections(ctx context.Context, key networkid.PortalKey, folded []foldedCorrection) {
	kv := x.kv()
	if kv == nil {
		return
	}
	for k, v := range foldedCorrectionKeys(x.loginID, key.ID, folded) {
		kv.Set(ctx, k, v)
	}
}

// foldedCorrectionKeys maps the dedupe keys of folded corrections to the bridgev2 id of their original.
func foldedCorrectionKeys(login networkid.UserLoginID, portal networkid.PortalID, folded []foldedCorrection) map[database.Key]string {
	out := map[database.Key]string{}
	for _, f := range folded {
		id := mamCorrectionID(f.corr)
		if id == "" {
			continue
		}
		origID := f.orig.StanzaID
		if origID == "" {
			origID = "xmp:" + f.orig.Timestamp.Format("20060102150405.000")
		}
		for _, sc := range senderScopes(bareJID(f.corr.From), "") {
			out[correctedKey(login, portal, sc, id)] = origID
		}
	}
	return out
}

// firstNonEmpty returns the first non-empty string, or "".
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
