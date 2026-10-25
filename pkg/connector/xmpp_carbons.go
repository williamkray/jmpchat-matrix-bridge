// Message Carbons (XEP-0280) support: receive copies of messages sent/received
// on other resources so the bridge can show them in the Matrix room immediately.

package connector

import (
	"context"
	"encoding/xml"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmlstream"
)

const carbonsNS = "urn:xmpp:carbons:2"

// isOwnAccountFrom reports whether the `from` of an outer stanza is acceptable as coming from our
// own account: empty (the server stamps it, RFC 6120) or a JID whose bare part equals ours
// (case-insensitive). XEP-0280 section 11 and XEP-0313 require carbons and archive results to come
// from the account's bare JID; anything else (a contact, a MUC occupant) is a forgery attempt.
func isOwnAccountFrom(from string, loginID networkid.UserLoginID) bool {
	if from == "" {
		return true
	}
	// XEP-0280 §11 / XEP-0313: copies and user-archive results come from the bare JID.
	return !strings.Contains(from, "/") && strings.EqualFold(from, bareJID(string(loginID)))
}

// carbonOuterFromOK validates the outer <message from> of a carbon copy and logs (no content) when
// it is dropped.
func (x *xmppClient) carbonOuterFromOK(outer *xml.StartElement, kind string) bool {
	from := ""
	if outer != nil {
		from = attrValue(outer.Attr, "from")
	}
	if isOwnAccountFrom(from, x.loginID) {
		return true
	}
	x.log.Debug().Str("carbon", kind).Str("outer_from", from).Msg("Dropping carbon with outer from that is not our own account")
	return false
}

// parseInnerMessageFromCarbon reads the inner <message> from a carbon copy and returns
// from, to, type, id, body. Per XEP-0280/0297, the stream is <received><forwarded><message>...,
// so we capture from/to/id from the first <message> element we encounter (at any depth).
func parseInnerMessageFromCarbon(t xmlstream.TokenReadEncoder) (from, to, typeAttr, id, body string, jmi *jingleMessageInfo, ids messageIDs, err error) {
	depth := 0
	var bodyBuf strings.Builder
	bodyDepth := 0 // depth of the direct-child <body> being read; 0 when not inside one
	msgDepth := 0  // depth of the first inner <message>; its direct children carry the XEP-0359 ids
	for {
		tok, err := t.Token()
		if err != nil {
			return from, to, typeAttr, id, body, jmi, ids, err
		}
		switch v := tok.(type) {
		case xml.StartElement:
			depth++
			if msgDepth > 0 {
				ids.track(v, depth, msgDepth+1)
			}
			jmi = detectJingleMessage(jmi, v)
			if v.Name.Local == "message" && msgDepth == 0 {
				msgDepth = depth
			}
			if v.Name.Local == "message" && from == "" {
				from = attrValue(v.Attr, "from")
				to = attrValue(v.Attr, "to")
				typeAttr = attrValue(v.Attr, "type")
				id = attrValue(v.Attr, "id")
			}
			if v.Name.Local == "body" && msgDepth > 0 && depth == msgDepth+1 && bodyDepth == 0 {
				bodyDepth = depth // only the message's own direct-child <body>, not the XEP-0428 fallback's
				bodyBuf.Reset()
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
			if depth == 0 {
				ids.finish()
				if ids.Retract != nil {
					body = "" // XEP-0424 fallback text; never bridge it
				}
				return from, to, typeAttr, id, body, jmi, ids, nil
			}
		}
	}
}

// consumeUntilEndElement reads tokens until the closing </localName> (we're already inside the element).
func consumeUntilEndElement(t xmlstream.TokenReadEncoder, localName string) {
	for {
		tok, err := t.Token()
		if err != nil {
			return
		}
		if ee, ok := tok.(xml.EndElement); ok && ee.Name.Local == localName {
			return
		}
	}
}

// handleCarbonSent processes a <sent> carbon: message we sent from another client.
// Inner message: from=our JID, to=contact. We queue it so it appears in the portal as "from me".
func (x *xmppClient) handleCarbonSent(t xmlstream.TokenReadEncoder, outer *xml.StartElement) error {
	if x.login == nil || !x.carbonOuterFromOK(outer, "sent") {
		consumeUntilEndElement(t, "sent")
		return nil
	}
	from, to, typeAttr, id, body, jmi, ids, err := parseInnerMessageFromCarbon(t)
	consumeUntilEndElement(t, "sent")
	if err != nil {
		x.log.Debug().Err(err).Msg("Failed to parse carbon <sent> inner message")
		return nil
	}
	// A retraction made from another of our clients (outer from was validated above).
	if ids.Retract != nil {
		if toBare := strings.ToLower(bareJID(to)); toBare != "" && typeAttr != "groupchat" {
			x.handleInboundRetraction(retraction{
				info: *ids.Retract, channel: retractChanCarbonSent, msgType: typeAttr, fromFull: from,
			}, networkid.PortalKey{ID: networkid.PortalID(toBare), Receiver: x.loginID}, id, ids, string(x.loginID))
		}
		return nil
	}
	// A correction made from another of our clients (outer from was validated above).
	if ids.Extras.Replace != "" {
		if toBare := strings.ToLower(bareJID(to)); toBare != "" && typeAttr != "groupchat" && body != "" {
			x.handleInboundCorrection(correction{
				claim:     senderClaim{channel: retractChanCarbonSent, msgType: typeAttr, fromFull: from},
				target:    ids.Extras.Replace,
				encrypted: ids.Extras.Encryption != "",
				corrID:    firstNonEmpty(id, ids.OriginID),
				body:      body,
				hasOOB:    false,
				senderID:  string(x.loginID),
				ts:        time.Now(),
			}, networkid.PortalKey{ID: networkid.PortalID(toBare), Receiver: x.loginID}, x.makeEventSender(string(x.loginID)))
		}
		return nil
	}
	// XEP-0353 action we performed from another of our devices (accept/reject/proceed).
	if jmi != nil {
		ctx, span := startReceiveSpan("carbon_sent", "", from, "", id, false, false, false)
		defer span.End()
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.jingle.action", jmi.action))
		}
		x.handleJingleMessageInitiation(ctx, *jmi, bareJID(from), true)
		return nil
	}
	_ = from
	toBare := to
	if idx := strings.Index(toBare, "/"); idx >= 0 {
		toBare = toBare[:idx]
	}
	if toBare == "" {
		return nil
	}
	if typeAttr != "groupchat" {
		x.handleInboundSignals(inboundSignal{
			key:      networkid.PortalKey{ID: networkid.PortalID(strings.ToLower(toBare)), Receiver: x.loginID},
			channel:  retractChanCarbonSent,
			fromFull: from,
			sender:   x.makeEventSender(string(x.loginID)),
			fromMe:   true,
			hasBody:  body != "",
			extras:   ids.Extras,
		})
	}
	// Skip carbon copies of chat state notifications (composing, paused, etc.) - no body.
	if body == "" {
		return nil
	}
	ctx, span := startReceiveSpan("carbon_sent", "", from, toBare, id, false, false, false)
	defer span.End()
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(strings.ToLower(toBare)),
		Receiver: x.loginID,
	}
	x.addPortalIDForMAMCatchUp(string(portalKey.ID))
	// The inner message's direct-child delay (offline storage) is the message time.
	now := time.Now()
	delay := trustedDelay(ids.Extras, now, dmDelayTrust(x.ownScope(), toBare))
	ts := delay
	if ts.IsZero() {
		ts = now
	}
	msgID := inboundMessageID(inboundIDs{
		idAttr: id, originID: ids.OriginID, stanzaID: ids.stanzaIDBy(string(x.loginID)), scope: x.ownScope(),
		delay: delay, senderFull: from, body: body,
	})
	sender := x.makeEventSender(string(x.loginID)) // "from me" for puppeting
	x.maybeTriggerBackfillOnNewPortal(ctx, portalKey)
	x.logFor(ctx).Debug().
		Str("to", toBare).
		Int("body_len", len(body)).
		Msg("Carbon <sent>: queuing message sent from another client")
	encKind, qBody, _ := applyEncryption(ids.Extras, body, "")
	x.queueRemoteEvent(ctx, &jabberRemoteMessage{
		portalKey: portalKey,
		sender:    sender,
		id:        msgID,
		encrypted: encKind,
		body:      qBody,
		timestamp: ts,
		idAttr:    id,
		x:         x,
		stanzaID:  ids.stanzaIDBy(string(x.loginID)),
		originID:  ids.OriginID,
		markable:  ids.Extras.Markable,
	})
	return nil
}

// handleCarbonReceived processes a <received> carbon: message we received on another client.
// Inner message: from=contact, to=our JID. Queue so it appears in the portal (may dedupe if we already got it).
func (x *xmppClient) handleCarbonReceived(t xmlstream.TokenReadEncoder, outer *xml.StartElement) error {
	if x.login == nil || !x.carbonOuterFromOK(outer, "received") {
		consumeUntilEndElement(t, "received")
		return nil
	}
	from, _, typeAttr, id, body, jmi, ids, err := parseInnerMessageFromCarbon(t)
	consumeUntilEndElement(t, "received")
	if err != nil {
		x.log.Debug().Err(err).Msg("Failed to parse carbon <received> inner message")
		return nil
	}
	// A retraction received on another of our clients (outer from was validated above).
	if ids.Retract != nil {
		if fromBare := strings.ToLower(bareJID(from)); fromBare != "" && typeAttr != "groupchat" {
			x.handleInboundRetraction(retraction{
				info: *ids.Retract, channel: retractChanCarbonReceived, msgType: typeAttr, fromFull: from,
			}, networkid.PortalKey{ID: networkid.PortalID(fromBare), Receiver: x.loginID}, id, ids, string(x.loginID))
		}
		return nil
	}
	// A correction received on another of our clients (outer from was validated above).
	if ids.Extras.Replace != "" {
		if fromBare := bareJID(from); fromBare != "" && typeAttr != "groupchat" && body != "" {
			x.handleInboundCorrection(correction{
				claim:     senderClaim{channel: retractChanCarbonReceived, msgType: typeAttr, fromFull: from},
				target:    ids.Extras.Replace,
				encrypted: ids.Extras.Encryption != "",
				corrID:    firstNonEmpty(id, ids.OriginID),
				body:      body,
				senderID:  fromBare,
				ts:        time.Now(),
			}, networkid.PortalKey{ID: networkid.PortalID(strings.ToLower(fromBare)), Receiver: x.loginID}, x.makeEventSender(fromBare))
		}
		return nil
	}
	// XEP-0353 call signalling received on another device (e.g. propose/retract from the caller).
	if jmi != nil {
		ctx, span := startReceiveSpan("carbon_received", "", from, "", id, false, false, false)
		defer span.End()
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.jingle.action", jmi.action))
		}
		x.handleJingleMessageInitiation(ctx, *jmi, bareJID(from), bareJID(from) == bareJID(string(x.loginID)))
		return nil
	}
	fromBare := from
	if idx := strings.Index(fromBare, "/"); idx >= 0 {
		fromBare = fromBare[:idx]
	}
	if fromBare == "" {
		return nil
	}
	if typeAttr != "groupchat" {
		x.handleInboundSignals(inboundSignal{
			key:      networkid.PortalKey{ID: networkid.PortalID(strings.ToLower(fromBare)), Receiver: x.loginID},
			channel:  retractChanCarbonReceived,
			fromFull: from,
			sender:   x.makeEventSender(strings.ToLower(fromBare)),
			hasBody:  body != "",
			extras:   ids.Extras,
		})
	}
	// Skip carbon copies of chat state notifications (composing, paused, etc.) - no body.
	if body == "" {
		return nil
	}
	ctx, span := startReceiveSpan("carbon_received", "", from, fromBare, id, false, false, false)
	defer span.End()
	portalKey := networkid.PortalKey{
		ID:       networkid.PortalID(strings.ToLower(fromBare)),
		Receiver: x.loginID,
	}
	x.addPortalIDForMAMCatchUp(string(portalKey.ID))
	// The inner message's direct-child delay (offline storage) is the message time.
	now := time.Now()
	delay := trustedDelay(ids.Extras, now, dmDelayTrust(x.ownScope(), fromBare))
	ts := delay
	if ts.IsZero() {
		ts = now
	}
	msgID := inboundMessageID(inboundIDs{
		idAttr: id, originID: ids.OriginID, stanzaID: ids.stanzaIDBy(string(x.loginID)), scope: x.ownScope(),
		delay: delay, senderFull: from, body: body,
	})
	sender := x.makeEventSender(fromBare)
	x.maybeTriggerBackfillOnNewPortal(ctx, portalKey)
	zerolog.Ctx(context.Background()).Debug().
		Str("from", fromBare).
		Int("body_len", len(body)).
		Msg("Carbon <received>: queuing message received on another client")
	encKind, qBody, _ := applyEncryption(ids.Extras, body, "")
	x.queueRemoteEvent(ctx, &jabberRemoteMessage{
		portalKey: portalKey,
		sender:    sender,
		id:        msgID,
		encrypted: encKind,
		body:      qBody,
		timestamp: ts,
		idAttr:    id,
		x:         x,
		stanzaID:  ids.stanzaIDBy(string(x.loginID)),
		originID:  ids.OriginID,
		markable:  ids.Extras.Markable,
	})
	return nil
}
