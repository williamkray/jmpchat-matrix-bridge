// MAM (XEP-0313 Message Archive Management) support for backfill and catch-up.
// Used for: (1) backfilling history when the bridge requests it; (2) ensuring
// messages sent from other Jabber clients appear in the Matrix room (catch-up).

package connector

import (
	"context"
	"encoding/xml"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/xid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"mellium.im/xmlstream"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

const (
	mamNS     = "urn:xmpp:mam:2"
	forwardNS = "urn:xmpp:forward:0"
	delayNS   = "urn:xmpp:delay"
	rsmNS     = "http://jabber.org/protocol/rsm"
	xdataNS   = "jabber:x:data"
)

// mamPendingEntry holds the channel and metadata for an in-flight MAM query.
type mamPendingEntry struct {
	ch      chan mamResultItem
	portal  networkid.PortalKey
	forward bool
}

type mamResultItem struct {
	From     string
	To       string
	Type     string
	StanzaID string // inner message `id` attribute; empty when missing (the row id then comes from mamItemMessageID)
	// ArchiveID is the <result id> (the archive's stanza-id, assigned by our own server); OriginID is
	// the inner XEP-0359 origin-id. Both are only populated for results that passed the outer-from check.
	ArchiveID string
	OriginID  string
	Body      string
	Timestamp time.Time
	QueryID   string
	// Retract is set when the archived message is a retraction/moderation (its fallback body is
	// dropped); Retracted marks an archived original that the server tombstoned (<retracted>).
	Retract   *retractInfo
	Retracted bool
	// Extras are the XEP-0308/0085/0333/0184/subject/E2EE signals of the archived message.
	Extras messageExtras
}

// stableIDs returns the stanza-id/origin-id to persist for an archived message: the archive id of a
// result from our own account, and the origin-id when it differs from the message id.
func (i mamResultItem) stableIDs() (stanzaID, originID string) {
	if i.OriginID != i.StanzaID {
		originID = i.OriginID
	}
	return i.ArchiveID, originID
}

// mamState holds MAM-related state for the client.
type mamState struct {
	mu      sync.Mutex
	pending map[string]*mamPendingEntry
}

func (m *mamState) register(queryID string, portal networkid.PortalKey, forward bool) chan mamResultItem {
	ch := make(chan mamResultItem, 50)
	m.mu.Lock()
	if m.pending == nil {
		m.pending = make(map[string]*mamPendingEntry)
	}
	m.pending[queryID] = &mamPendingEntry{ch: ch, portal: portal, forward: forward}
	m.mu.Unlock()
	return ch
}

func (m *mamState) deliverMessage(queryID string, item mamResultItem) bool {
	m.mu.Lock()
	entry := m.pending[queryID]
	m.mu.Unlock()
	if entry == nil {
		return false
	}
	select {
	case entry.ch <- item:
		return true
	default:
		return false
	}
}

func (m *mamState) closeAndRemove(queryID string) {
	m.mu.Lock()
	entry := m.pending[queryID]
	if entry != nil {
		delete(m.pending, queryID)
		close(entry.ch)
	}
	m.mu.Unlock()
}

// mamFinHandler adapts xmppClient for mux.IQ so MAM <fin> results close the pending channel.
type mamFinHandler struct{ x *xmppClient }

func (h mamFinHandler) HandleIQ(iq stanza.IQ, _ xmlstream.TokenReadEncoder, _ *xml.StartElement) error {
	id := string(iq.ID)
	if strings.HasPrefix(id, "mam-") {
		queryID := strings.TrimPrefix(id, "mam-")
		h.x.mam.closeAndRemove(queryID)
	}
	return nil
}

// handleMAMResult is called when the message handler sees a MAM result wrapper.
// It parses the forwarded message and delivers it to the pending MAM collector.
//
// outer is the enclosing <message>; results are only accepted when its `from` is empty or our own
// bare JID (the user archive answers from the account), so a contact cannot inject forged history.
func (x *xmppClient) handleMAMResult(t xmlstream.TokenReadEncoder, start *xml.StartElement, outer *xml.StartElement) error {
	queryID := attrValue(start.Attr, "queryid")
	if queryID == "" {
		return nil
	}
	outerFrom := ""
	if outer != nil {
		outerFrom = attrValue(outer.Attr, "from")
	}
	if !isOwnAccountFrom(outerFrom, x.loginID) {
		x.log.Debug().Str("outer_from", outerFrom).Msg("Dropping MAM result with outer from that is not our own account")
		consumeMAMResult(t)
		return nil
	}
	// Parse <forwarded xmlns='urn:xmpp:forward:0'><message .../><delay .../></forwarded>
	var from, to, msgType, stanzaID, body string
	var timestamp time.Time
	var ids messageIDs
	var bodyBuf strings.Builder
	bodyDepth := 0 // depth of the direct-child <body> being read; 0 when not inside one
	msgDepth := 0  // depth of the first inner <message>; its direct children carry the XEP-0359 ids
	depth := 1
	for depth > 0 {
		tok, err := t.Token()
		if err != nil {
			return err
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			if msgDepth > 0 {
				ids.track(v, depth, msgDepth+1)
			}
			if v.Name.Local == "message" && msgDepth == 0 {
				msgDepth = depth
			}
			// Inner message may have xmlns="jabber:client" (XEP-0297) or inherit forwardNS; accept either.
			if v.Name.Local == "message" && from == "" {
				from = attrValue(v.Attr, "from")
				to = attrValue(v.Attr, "to")
				msgType = attrValue(v.Attr, "type")
				stanzaID = attrValue(v.Attr, "id")
			}
			if v.Name.Local == "body" && msgDepth > 0 && depth == msgDepth+1 && bodyDepth == 0 {
				bodyDepth = depth // only the message's own direct-child <body>, not the XEP-0428 fallback's
				bodyBuf.Reset()
			}
			if v.Name.Space == delayNS && v.Name.Local == "delay" {
				if stamp := attrValue(v.Attr, "stamp"); stamp != "" {
					// XEP-0082: CCYY-MM-DDThh:mm:ss[.sss]Z
					if t, err := time.Parse(time.RFC3339, stamp); err == nil {
						timestamp = t
					}
				}
			}
		case xml.CharData:
			if bodyDepth > 0 {
				bodyBuf.Write(v)
			}
			if msgDepth > 0 {
				ids.trackChars(v)
			}
		case xml.EndElement:
			if bodyDepth > 0 && depth == bodyDepth {
				body = strings.TrimSpace(bodyBuf.String())
				bodyDepth = 0
			}
			depth--
			if msgDepth > 0 {
				ids.trackEnd(depth + 1)
			}
		}
	}
	ids.finish()
	if ids.Retract != nil || ids.inviteFrom(from) != nil {
		body = "" // XEP-0424 / MUC invite fallback text; never bridge it
	}
	if timestamp.IsZero() {
		timestamp = time.Now()
	}
	archiveID := attrValue(start.Attr, "id")
	if archiveID == "" {
		archiveID = ids.stanzaIDBy(string(x.loginID))
	}
	x.mam.deliverMessage(queryID, mamResultItem{
		From: from, To: to, Type: msgType, StanzaID: stanzaID, ArchiveID: archiveID, OriginID: ids.OriginID,
		Body: body, Timestamp: timestamp, QueryID: queryID,
		Retract: ids.Retract, Retracted: ids.Tombstone,
		Extras: ids.Extras,
	})
	return nil
}

// consumeMAMResult discards the remainder of a <result> element (we are already inside it).
func consumeMAMResult(t xmlstream.TokenReadEncoder) {
	for depth := 1; depth > 0; {
		tok, err := t.Token()
		if err != nil {
			return
		}
		switch tok.(type) {
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}
}

// mamQueryPayload returns a TokenReader for the MAM query element (XEP-0313).
func mamQueryPayload(queryID, withJID string, start, end *time.Time, max int) xml.TokenReader {
	qStart := xml.StartElement{
		Name: xml.Name{Space: mamNS, Local: "query"},
		Attr: []xml.Attr{{Name: xml.Name{Local: "queryid"}, Value: queryID}},
	}
	var formTokens []xml.Token
	formFields := []string{"FORM_TYPE", "urn:xmpp:mam:2"}
	if start != nil {
		formFields = append(formFields, "start", start.UTC().Format(time.RFC3339))
	}
	if end != nil {
		formFields = append(formFields, "end", end.UTC().Format(time.RFC3339))
	}
	if withJID != "" {
		formFields = append(formFields, "with", withJID)
	}
	xStart := xml.StartElement{Name: xml.Name{Space: xdataNS, Local: "x"}, Attr: []xml.Attr{{Name: xml.Name{Local: "type"}, Value: "submit"}}}
	formTokens = append(formTokens, xStart)
	for i := 0; i < len(formFields); i += 2 {
		varName := formFields[i]
		varValue := ""
		if i+1 < len(formFields) {
			varValue = formFields[i+1]
		}
		fieldStart := xml.StartElement{Name: xml.Name{Space: xdataNS, Local: "field"}, Attr: []xml.Attr{{Name: xml.Name{Local: "var"}, Value: varName}}}
		valueStart := xml.StartElement{Name: xml.Name{Space: xdataNS, Local: "value"}}
		formTokens = append(formTokens, fieldStart, valueStart, xml.CharData(varValue), valueStart.End(), fieldStart.End())
	}
	formTokens = append(formTokens, xStart.End())

	var rsmTokens []xml.Token
	if max > 0 {
		setStart := xml.StartElement{Name: xml.Name{Space: rsmNS, Local: "set"}}
		maxStart := xml.StartElement{Name: xml.Name{Space: rsmNS, Local: "max"}}
		rsmTokens = append(rsmTokens, setStart, maxStart, xml.CharData(fmt.Sprintf("%d", max)), maxStart.End(), setStart.End())
	}

	readers := make([]xml.TokenReader, 0, 3)
	readers = append(readers, xmlstream.Token(qStart))
	for _, t := range formTokens {
		readers = append(readers, xmlstream.Token(t))
	}
	for _, t := range rsmTokens {
		readers = append(readers, xmlstream.Token(t))
	}
	readers = append(readers, xmlstream.Token(qStart.End()))
	return xmlstream.MultiReader(readers...)
}

// fetchMAMItems runs a MAM query and returns the collected items. Used by FetchMessages and by periodic catch-up.
func (x *xmppClient) fetchMAMItems(ctx context.Context, portalKey networkid.PortalKey, withJID string, start, end time.Time, max int) (items []mamResultItem, err error) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}
	local := sess.LocalAddr()
	if local.String() == "" {
		return nil, fmt.Errorf("no local JID for MAM query")
	}
	ctx, span := telemetry.StartChild(ctx, "xmpp.iq.mam_query", trace.WithSpanKind(trace.SpanKindClient))
	defer func() {
		if span.IsRecording() {
			span.SetAttributes(
				attribute.String("xmpp.peer.jid", withJID),
				attribute.String("xmpp.iq.namespace", mamNS),
				attribute.Int("mam.max", max),
				attribute.Int("mam.items", len(items)),
			)
		}
		if err != nil {
			telemetry.Fail(span, err)
		}
		span.End()
	}()
	// XEP-0313: user archive is queried at the account's bare JID.
	toJID := local.Bare()
	queryID := xid.New().String()
	ch := x.mam.register(queryID, portalKey, true)
	defer x.mam.closeAndRemove(queryID)
	iqID := "mam-" + queryID
	payload := mamQueryPayload(queryID, withJID, &start, &end, max)
	iq := stanza.IQ{ID: iqID, Type: stanza.SetIQ, To: toJID}
	if err := sess.Send(ctx, iq.Wrap(payload)); err != nil {
		return nil, err
	}
	timeout := time.NewTimer(25 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout.C:
			span.SetAttributes(attribute.Bool("mam.timed_out", true))
			return items, nil
		case item, ok := <-ch:
			if !ok {
				return items, nil
			}
			items = append(items, item)
		}
	}
}

// FetchMessages implements BackfillingNetworkAPI. It uses MAM (XEP-0313) when the server
// supports it to backfill history and to catch up messages sent from other Jabber clients.
func (x *xmppClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if !x.IsLoggedIn() {
		return nil, bridgev2.ErrNotLoggedIn
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, bridgev2.ErrNotLoggedIn
	}

	portalID := string(params.Portal.ID)
	if portalID == "" {
		return nil, nil
	}
	ctx, span := telemetry.Start(ctx, "matrix.fetch_messages")
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.peer.jid", portalID),
			attribute.Bool("forward", params.Forward),
			attribute.Int("count", params.Count),
		)
	}
	log.Info().
		Str("portal_id", portalID).
		Bool("forward", params.Forward).
		Int("count", params.Count).
		Msg("Backfill FetchMessages called by bridge")
	x.addPortalIDForMAMCatchUp(portalID)

	// Only support DM for now; MUC MAM may use different with JID (room@service).
	withJID := portalID
	var anchorTime time.Time
	if params.AnchorMessage != nil {
		anchorTime = params.AnchorMessage.Timestamp
	}
	count := params.Count
	if count <= 0 {
		count = 50
	}
	var start, end time.Time
	if params.Forward {
		start = anchorTime
		end = time.Now()
	} else {
		end = anchorTime
		if anchorTime.IsZero() {
			end = time.Now()
		}
		start = end.Add(-365 * 24 * time.Hour)
	}

	items, err := x.fetchMAMItems(ctx, params.Portal.PortalKey, withJID, start, end, count+1)
	if err != nil {
		log.Warn().Err(err).Str("portal_id", portalID).Msg("MAM query failed")
		telemetry.Fail(span, err)
		return nil, err
	}
	span.SetAttributes(attribute.Int("items", len(items)))
	log.Info().Str("portal_id", portalID).Int("mam_items", len(items)).Msg("MAM backfill query returned")

	// Convert to BackfillMessage; respect count (we may have requested count+1 to detect hasMore).
	hasMore := len(items) > count
	// Retractions are newer than their originals and the first thing truncation drops, so collect
	// them from everything fetched.
	batchRetracted := collectBatchRetractions(items)
	// Corrections are newer than their originals too: fold them into the originals of this batch (a
	// BackfillMessage cannot carry an edit) before truncating.
	items, foldedCorrs := foldBatchCorrectionsDetailed(items)
	if len(items) > count {
		items = items[:count]
	}
	// Dedupe keys only for corrections whose original is within the kept items: a correction folded into
	// an original that truncation dropped is still pending for the next page.
	x.recordFoldedCorrections(ctx, params.Portal.PortalKey, foldedWithin(foldedCorrs, items))
	// MAM server order is implementation-defined; sort by timestamp so Matrix always gets oldest-first.
	if len(items) > 1 {
		sort.Slice(items, func(i, j int) bool { return items[i].Timestamp.Before(items[j].Timestamp) })
	}

	portalKey := params.Portal.PortalKey
	messages := make([]*bridgev2.BackfillMessage, 0, len(items))
	for _, item := range items {
		fromBare := item.From
		if idx := strings.Index(item.From, "/"); idx >= 0 {
			fromBare = item.From[:idx]
		}
		// Skip groupchat for now if we ever get MUC MAM
		if item.Type == "groupchat" {
			continue
		}
		// Retractions themselves (their fallback text is gone), server tombstones of retracted
		// originals and body-less items (chat states) are never backfilled, nor are originals that a
		// retraction in this batch or an earlier one removed.
		if !mamItemBackfillable(item, nil) || x.mamItemHidden(ctx, portalKey, item, batchRetracted) {
			continue
		}
		sender := x.makeEventSender(fromBare)
		msgID := x.mamItemMessageID(item)
		// Already bridged (live copy, earlier backfill): not backfilled a second time.
		if x.mamItemAlreadyBridged(ctx, portalKey, item, msgID, sender) {
			continue
		}
		stanzaID, originID := item.stableIDs()
		if originID == string(msgID) {
			originID = ""
		}
		content := mamBackfillContent(item)
		cm := &bridgev2.ConvertedMessage{
			Parts: []*bridgev2.ConvertedMessagePart{{
				Type:    event.EventMessage,
				Content: content,
				DBMetadata: &MessageMetadata{
					XMPPMessageID:  string(msgID),
					StanzaID:       stanzaID,
					OriginID:       originID,
					BackfillSource: "mam",
					Markable:       item.Extras.Markable,
				},
			}},
		}
		messages = append(messages, &bridgev2.BackfillMessage{
			ConvertedMessage: cm,
			Sender:           sender,
			ID:               msgID,
			TxnID:            networkid.TransactionID(msgID),
			Timestamp:        item.Timestamp,
			StreamOrder:      item.Timestamp.UnixMilli(),
		})
	}

	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		HasMore:  hasMore,
		Forward:  params.Forward,
		MarkRead: false,
	}, nil
}

const mamCatchUpInterval = 45 * time.Second
const mamCatchUpWindow = 2 * time.Minute // first run: messages from last 2 minutes

// runMAMCatchUpLoop runs periodic MAM catch-up so messages sent from other Jabber clients
// appear in the Matrix room within roughly one interval (e.g. 45s) when carbons are not used.
func (x *xmppClient) runMAMCatchUpLoop(ctx context.Context) {
	defer func() { x.log.Debug().Msg("MAM catch-up loop exited") }()
	ticker := time.NewTicker(mamCatchUpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !x.IsLoggedIn() || x.login == nil {
				continue
			}
			x.runMAMCatchUpOnce(ctx)
		}
	}
}

// addPortalIDForMAMCatchUp records a portal ID so we run MAM catch-up for it even if not in roster
// (e.g. Cheogram group MMS). Call when queueing any inbound or carbon message.
func (x *xmppClient) addPortalIDForMAMCatchUp(portalID string) {
	if portalID == "" {
		return
	}
	key := strings.ToLower(portalID)
	x.knownPortalIDsForMAMMu.Lock()
	if x.knownPortalIDsForMAM == nil {
		x.knownPortalIDsForMAM = make(map[string]struct{})
	}
	x.knownPortalIDsForMAM[key] = struct{}{}
	x.knownPortalIDsForMAMMu.Unlock()
}

// runMAMCatchUpOnce runs MAM catch-up for roster contacts and any portal we've seen (e.g. Cheogram groups).
func (x *xmppClient) runMAMCatchUpOnce(ctx context.Context) {
	jidSet := make(map[string]struct{})
	x.rosterMu.RLock()
	for j := range x.roster {
		jidSet[strings.ToLower(j)] = struct{}{}
	}
	x.rosterMu.RUnlock()
	x.knownPortalIDsForMAMMu.Lock()
	for j := range x.knownPortalIDsForMAM {
		jidSet[j] = struct{}{}
	}
	x.knownPortalIDsForMAMMu.Unlock()
	if len(jidSet) == 0 {
		return
	}
	cycleCtx, span := telemetry.Start(ctx, "mam.catchup.cycle")
	defer span.End()
	var checked, withItems, errCount int
	defer func() {
		span.SetAttributes(
			attribute.Int("portals.checked", checked),
			attribute.Int("portals.with_items", withItems),
			attribute.Int("errors", errCount),
		)
	}()
	jids := make([]string, 0, len(jidSet))
	for j := range jidSet {
		jids = append(jids, j)
	}
	withRoom, filter := x.roomPortalIDs(ctx)
	known := x.knownMAMSnapshot()
	var skipped int
	defer func() { span.SetAttributes(attribute.Int("portals.skipped", skipped)) }()
	for _, bareJID := range jids {
		if filter && !mamEligible(bareJID, withRoom, known) {
			skipped++
			continue
		}
		// ctx (span-less) is used for the work so xmpp.iq.mam_query creates no span; the
		// per-portal span is recorded post-hoc under cycleCtx only when something happened.
		res := x.mamCatchUpForPortal(ctx, cycleCtx, bareJID)
		checked++
		if res.items > 0 {
			withItems++
		}
		if res.failed {
			errCount++
		}
		// Brief yield so we don't hammer the server
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// runFullBackfill runs MAM backfill (mamBackfillOnView) for every roster DM JID, skipping MUCs.
// Used after login (when full_backfill_on_login is set) and when the user runs sync-roster (when backfill is enabled),
// so portals are created and history filled before new messages arrive (avoids newest message appearing above history).
// Respects backfill.enabled; no-ops if disabled. Throttles 200ms between JIDs.
func (x *xmppClient) runFullBackfill(ctx context.Context) {
	if x.login == nil || x.conn.backfillConfig == nil || !x.conn.backfillConfig.Enabled {
		return
	}
	x.rosterMu.RLock()
	jids := make([]string, 0, len(x.roster))
	for j := range x.roster {
		jids = append(jids, strings.ToLower(j))
	}
	x.rosterMu.RUnlock()
	if len(jids) == 0 {
		x.log.Debug().Msg("Full backfill: roster empty, nothing to do")
		return
	}
	ctx, span := telemetry.Start(ctx, "mam.backfill_full")
	defer span.End()
	log := x.logFor(ctx)
	span.SetAttributes(attribute.Int("roster.count", len(jids)))
	log.Info().Int("roster_count", len(jids)).Msg("Full backfill: starting MAM backfill for roster DMs")
	done, skipped := 0, 0
	defer func() {
		span.SetAttributes(attribute.Int("backfilled", done), attribute.Int("portals.skipped", skipped))
	}()
	withRoom, filter := x.roomPortalIDs(ctx)
	known := x.knownMAMSnapshot()
	for _, bareJID := range jids {
		if ctx.Err() != nil {
			break
		}
		if filter && !mamEligible(bareJID, withRoom, known) {
			skipped++
			continue
		}
		if x.isMUC(ctx, bareJID) {
			continue
		}
		x.mamBackfillOnView(ctx, bareJID)
		done++
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	log.Info().Int("backfilled_count", done).Msg("Full backfill: finished")
}

// mamCatchUpResult reports what a single portal catch-up did, for the cycle span.
type mamCatchUpResult struct {
	items  int
	failed bool
}

// mamCatchUpForPortal queries MAM for messages since last sync and queues new ones as remote events.
// ctx is span-less and used for the work; cycleCtx is the parent for the post-hoc
// mam.catchup.portal span, which is only recorded when items came back or the query failed.
func (x *xmppClient) mamCatchUpForPortal(ctx, cycleCtx context.Context, bareJID string) (res mamCatchUpResult) {
	start := time.Now()
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(bareJID),
		Receiver: x.loginID,
	}
	x.lastMAMSyncMu.Lock()
	lastSync := x.lastMAMSync[bareJID]
	x.lastMAMSyncMu.Unlock()
	if lastSync.IsZero() {
		lastSync = time.Now().Add(-mamCatchUpWindow)
	}
	end := time.Now()
	items, err := x.fetchMAMItems(ctx, portalKey, bareJID, lastSync, end, 50)
	if err != nil {
		x.logFor(cycleCtx).Debug().Err(err).Str("portal_id", bareJID).Msg("MAM catch-up query failed")
		_, pspan := telemetry.Start(cycleCtx, "mam.catchup.portal", trace.WithTimestamp(start))
		pspan.SetAttributes(attribute.String("xmpp.peer.jid", bareJID))
		telemetry.Fail(pspan, err)
		pspan.End()
		res.failed = true
		return res
	}
	res.items = len(items)
	if len(items) == 0 {
		x.lastMAMSyncMu.Lock()
		if x.lastMAMSync == nil {
			x.lastMAMSync = make(map[string]time.Time)
		}
		x.lastMAMSync[bareJID] = end
		x.lastMAMSyncMu.Unlock()
		return res
	}
	pctx, pspan := telemetry.Start(cycleCtx, "mam.catchup.portal", trace.WithTimestamp(start))
	defer pspan.End()
	queued := 0
	defer func() {
		if pspan.IsRecording() {
			pspan.SetAttributes(
				attribute.String("xmpp.peer.jid", bareJID),
				attribute.Int("items", len(items)),
				attribute.Int("queued", queued),
			)
		}
	}()
	batchRetracted := collectBatchRetractions(items)
	for _, item := range items {
		if item.Type == "groupchat" {
			continue
		}
		if x.queueMAMRetraction(pctx, portalKey, item) || x.queueMAMCorrection(pctx, portalKey, item) {
			continue
		}
		if item.Retracted || x.mamItemHidden(ctx, portalKey, item, batchRetracted) {
			continue
		}
		// Skip ephemeral/chat state notifications (no body).
		if item.Body == "" {
			continue
		}
		senderJID, bodyOut := ResolveCheogramGroupMMSSenderForMAM(item.From, item.Body)
		sender := x.makeEventSender(senderJID)
		msgID := x.mamItemMessageID(item)
		if x.mamItemAlreadyBridged(ctx, portalKey, item, msgID, sender) {
			continue
		}
		stanzaID, originID := item.stableIDs()
		ts := item.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		encKind, qBody, _ := applyEncryption(item.Extras, bodyOut, "")
		x.queueRemoteEvent(pctx, &jabberRemoteMessage{
			portalKey: portalKey,
			sender:    sender,
			id:        msgID,
			encrypted: encKind,
			body:      qBody,
			timestamp: ts,
			idAttr:    item.StanzaID,
			x:         x,
			stanzaID:  stanzaID,
			originID:  originID,
			markable:  item.Extras.Markable,

			backfillSource: "mam",
		})
		queued++
	}
	if queued > 0 {
		x.logFor(pctx).Debug().Str("portal_id", bareJID).Int("queued", queued).Msg("MAM catch-up: queued new messages")
	}
	x.lastMAMSyncMu.Lock()
	if x.lastMAMSync == nil {
		x.lastMAMSync = make(map[string]time.Time)
	}
	x.lastMAMSync[bareJID] = end
	x.lastMAMSyncMu.Unlock()
	return res
}

const mamBackfillWindow = 365 * 24 * time.Hour
const mamBackfillMaxMessagesDefault = 50

// maybeTriggerBackfillOnNewPortal is called when queueing an inbound remote event. If the portal
// has no Matrix room yet (MXID empty), the bridge will create one. Trigger MAM backfill in a
// goroutine—must be async because the XMPP handler must return for the mellium Serve loop to
// read IQ results; the portal lookup (DB) and portalIsMUC (disco#info IQ) would otherwise block
// the loop that has to deliver their responses. Only the cheap config guard runs synchronously.
// Respects backfill.enabled and skips MUC. ctx is reserved as the trace link origin; it is not
// used for cancellation. Several messages arriving before the room exists may each trigger a
// backfill (same race as before).
func (x *xmppClient) maybeTriggerBackfillOnNewPortal(ctx context.Context, portalKey networkid.PortalKey) {
	if x.login == nil || x.conn.backfillConfig == nil || !x.conn.backfillConfig.Enabled {
		return
	}
	// Captured before the goroutine; only its span context is read (link origin).
	origin := ctx
	go func() {
		lctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		portal, err := x.conn.br.GetPortalByKey(lctx, portalKey)
		if err != nil || portal == nil || portal.MXID != "" || x.portalIsMUC(lctx, portal) {
			return
		}
		bctx, span := telemetry.StartLinkedRoot(x.log.WithContext(context.Background()), "mam.backfill_on_view", origin,
			trace.WithAttributes(attribute.String("mam.trigger", "new_portal")))
		defer span.End()
		x.doMAMBackfillOnView(bctx, span, string(portalKey.ID), x.mamBackfillMaxMessages())
	}()
}

// backfillChatAsync starts the on-view MAM backfill of a freshly created 1:1 portal in its own
// goroutine and returns at once (the Dialpad widget's Message must not wait for history). It is the
// goroutine twin of maybeTriggerBackfillOnNewPortal without the portal and MUC checks: callers pass
// Cheogram 1:1 JIDs only. from is only the trace link origin, never used for cancellation; trigger is
// the mam.trigger attribute. No-op when backfill is disabled or the client has no login.
func (x *xmppClient) backfillChatAsync(from context.Context, portalID, trigger string) {
	if x.login == nil || x.conn == nil || x.conn.backfillConfig == nil || !x.conn.backfillConfig.Enabled {
		return
	}
	origin := from
	go func() {
		bctx, span := telemetry.StartLinkedRoot(x.log.WithContext(context.Background()), "mam.backfill_on_view", origin,
			trace.WithAttributes(attribute.String("mam.trigger", trigger)))
		defer span.End()
		x.doMAMBackfillOnView(bctx, span, portalID, x.mamBackfillMaxMessages())
	}()
}

// mamBackfillOnView fetches historical MAM messages for a DM portal and queues them as remote events.
// Called from HandleMatrixViewingChat when the framework never triggers FetchMessages (e.g. appservice
// flow, sync-portal, pm command). Only runs for DMs; MUC uses different MAM semantics.
// Respects config.backfill.enabled and config.backfill.max_initial_messages.
func (x *xmppClient) mamBackfillOnView(ctx context.Context, portalID string) {
	if portalID == "" || x.login == nil {
		return
	}
	bf := x.conn.backfillConfig
	if bf == nil || !bf.Enabled {
		return
	}
	ctx, span := telemetry.Start(ctx, "mam.backfill_on_view")
	defer span.End()
	x.doMAMBackfillOnView(ctx, span, portalID, x.mamBackfillMaxMessages())
}

// mamBackfillMaxMessages returns the configured initial backfill size (backfillConfig must be non-nil).
func (x *xmppClient) mamBackfillMaxMessages() int {
	if n := x.conn.backfillConfig.MaxInitialMessages; n > 0 {
		return n
	}
	return mamBackfillMaxMessagesDefault
}

// doMAMBackfillOnView is the body of mamBackfillOnView; span is the already-started
// mam.backfill_on_view span (a normal span, or a linked root from the new-portal trigger).
func (x *xmppClient) doMAMBackfillOnView(ctx context.Context, span trace.Span, portalID string, maxMsgs int) {
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.peer.jid", portalID))
	}
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(portalID),
		Receiver: x.loginID,
	}
	start := time.Now().Add(-mamBackfillWindow)
	end := time.Now()
	items, err := x.fetchMAMItems(ctx, portalKey, portalID, start, end, maxMsgs+1)
	if err != nil {
		log.Debug().Err(err).Str("portal_id", portalID).Msg("MAM backfill on view: query failed")
		telemetry.Fail(span, err)
		return
	}
	span.SetAttributes(attribute.Int("items", len(items)))
	if len(items) == 0 {
		return
	}
	// MAM server order is implementation-defined; sort by timestamp so we queue oldest-first for Matrix.
	if len(items) > 1 {
		sort.Slice(items, func(i, j int) bool { return items[i].Timestamp.Before(items[j].Timestamp) })
	}
	// Retractions are newer than their originals and the first thing truncation drops, so collect
	// them from everything fetched.
	batchRetracted := collectBatchRetractions(items)
	if len(items) > maxMsgs {
		items = items[:maxMsgs]
	}
	queued := 0
	defer func() { span.SetAttributes(attribute.Int("queued", queued)) }()
	for _, item := range items {
		if item.Type == "groupchat" {
			continue
		}
		if x.queueMAMRetraction(ctx, portalKey, item) || x.queueMAMCorrection(ctx, portalKey, item) {
			continue
		}
		if item.Body == "" || item.Retracted {
			continue
		}
		if x.mamItemHidden(ctx, portalKey, item, batchRetracted) {
			continue
		}
		senderJID, bodyOut := ResolveCheogramGroupMMSSenderForMAM(item.From, item.Body)
		sender := x.makeEventSender(senderJID)
		msgID := x.mamItemMessageID(item)
		if x.mamItemAlreadyBridged(ctx, portalKey, item, msgID, sender) {
			continue
		}
		stanzaID, originID := item.stableIDs()
		ts := item.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		encKind, qBody, _ := applyEncryption(item.Extras, bodyOut, "")
		x.queueRemoteEvent(ctx, &jabberRemoteMessage{
			portalKey: portalKey,
			sender:    sender,
			id:        msgID,
			encrypted: encKind,
			body:      qBody,
			timestamp: ts,
			idAttr:    item.StanzaID,
			x:         x,
			stanzaID:  stanzaID,
			originID:  originID,
			markable:  item.Extras.Markable,

			backfillSource: "mam",
		})
		queued++
	}
	if queued > 0 {
		log.Info().Str("portal_id", portalID).Int("queued", queued).Msg("MAM backfill on view: queued historical messages")
	}
}

// mamEligible reports whether MAM catch-up / full backfill should query jid. Normal XMPP contacts
// (anything that is not a Cheogram 1:1 number JID) are always eligible. A Cheogram number is eligible
// only when it already has a Matrix room (withRoom: lowercased portal IDs of this login with an MXID)
// or was seen on this connection (known: knownPortalIDsForMAM). This keeps bulk-imported numbers
// (CardDAV sync) from being polled every cycle and from having up to a year of texts replayed into
// new portals on every connect; the first inbound message creates the portal as usual.
func mamEligible(jid string, withRoom, known map[string]struct{}) bool {
	if _, isNumber := cheogramE164(jid); !isNumber {
		return true
	}
	key := strings.ToLower(jid)
	if _, ok := withRoom[key]; ok {
		return true
	}
	_, ok := known[key]
	return ok
}

// roomPortalIDs returns the lowercased IDs of this login's portals that have a Matrix room, using a
// single DB query (no per-JID portal cache lock). On error it returns nil and false so callers can
// fall back to the unfiltered behaviour instead of silently dropping catch-up.
func (x *xmppClient) roomPortalIDs(ctx context.Context) (map[string]struct{}, bool) {
	if x.conn == nil || x.conn.br == nil {
		return nil, false
	}
	portals, err := x.conn.br.DB.Portal.GetAllWithMXID(ctx)
	if err != nil {
		x.logFor(ctx).Debug().Err(err).Msg("MAM: failed to list portals; not filtering numbers")
		return nil, false
	}
	set := make(map[string]struct{}, len(portals))
	for _, p := range portals {
		if p.Receiver == x.loginID {
			set[strings.ToLower(string(p.ID))] = struct{}{}
		}
	}
	return set, true
}

// knownMAMSnapshot copies knownPortalIDsForMAM.
func (x *xmppClient) knownMAMSnapshot() map[string]struct{} {
	x.knownPortalIDsForMAMMu.Lock()
	defer x.knownPortalIDsForMAMMu.Unlock()
	set := make(map[string]struct{}, len(x.knownPortalIDsForMAM))
	for j := range x.knownPortalIDsForMAM {
		set[j] = struct{}{}
	}
	return set
}
