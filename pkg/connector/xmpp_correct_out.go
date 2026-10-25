package connector

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Outbound XEP-0308 corrections (Matrix edit -> <replace>). bridgev2 calls HandleMatrixEdit only for
// edits that originate in Matrix; remote corrections never come back through here, so there is no loop.

// smsEditPrefix starts the body of an edit sent to a Cheogram (SMS/MMS) JID: SMS cannot be corrected, so
// the new text goes out as a fresh message marked with the usual "* corrected text" convention.
const smsEditPrefix = "* "

var (
	errEditNotOwn    = errors.New("Only your own messages can be edited on XMPP.")
	errEditMedia     = errors.New("Editing files or captions is not supported on XMPP.")
	errEditNoXMPPRef = errors.New("Cannot edit: this message has no XMPP id.")
	errEditNoText    = errors.New("Cannot edit: the new message has no text.")
)

// sendOut sends m on the current session (or hands it to the test hook).
func (x *xmppClient) sendOut(ctx context.Context, m outMessage) (string, error) {
	m = x.withSignalFlags(m)
	if x.sendOutHook != nil {
		id, err := x.sendOutHook(ctx, m)
		if err == nil {
			x.noteOutboundSent(m)
		}
		return id, err
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return "", errRetractNotConnected
	}
	id, err := sendXMPPOut(ctx, sess, m)
	if err == nil {
		x.noteOutboundSent(m)
	}
	return id, err
}

// HandleMatrixEdit implements bridgev2.EditHandlingNetworkAPI.
func (x *xmppClient) HandleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	ctx, span := telemetry.Start(ctx, "matrix.handle_edit", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.peer.jid", string(msg.Portal.ID)))
		setPortalTypeAttr(span, msg.Portal)
		if msg.Portal.MXID != "" {
			span.SetAttributes(attribute.String("matrix.room_id", msg.Portal.MXID.String()))
		}
		if msg.Event != nil {
			span.SetAttributes(attribute.String("matrix.event_id", string(msg.Event.ID)))
		}
	}
	err := x.handleMatrixEdit(ctx, msg)
	telemetry.Fail(span, err)
	return err
}

func (x *xmppClient) handleMatrixEdit(ctx context.Context, msg *bridgev2.MatrixEdit) error {
	log := x.logFor(ctx)
	target := msg.EditTarget
	portal := msg.Portal
	portalID := string(portal.ID)
	fail := func(err error) error { return bridgev2.WrapErrorInStatus(err).WithSendNotice(true) }

	if !isOwnMessage(target, x.loginID) {
		return fail(errEditNotOwn)
	}
	// Relay mode: every relayed message is stored under the relay login, so a relayed Matrix user may
	// only edit a message that they sent themselves.
	if msg.OrigSender != nil && (msg.Event == nil || target.SenderMXID == "" || target.SenderMXID != msg.Event.Sender) {
		return fail(errEditNotOwn)
	}
	if msg.Content.MsgType.IsMedia() {
		return fail(fmt.Errorf("%w: %s", bridgev2.ErrUnsupportedMessageType, msg.Content.MsgType))
	}
	meta := targetMeta(target)
	if meta != nil && meta.Media {
		return fail(errEditMedia)
	}
	// Never send HTML as the XMPP body: derive plain text from the formatted body instead; mentions of
	// bridged users use their XMPP names.
	body := rewriteMentions(msg.Content.Body, msg.Content.FormattedBody, msg.Content.Format == event.FormatHTML, x.mentionResolver(ctx, msg.Portal))
	if body == "" && msg.Content.FormattedBody != "" {
		body = plainFromFormattedBody(msg.Content.FormattedBody)
	}
	if body == "" {
		return fail(errEditNoText)
	}
	toJID, err := jid.Parse(portalID)
	if err != nil {
		return fail(fmt.Errorf("invalid portal JID format"))
	}

	// SMS/MMS cannot be corrected: send the new text as a NEW plain message ("* text", new id, no
	// <replace>). The Matrix edit stands and the new id is not stored as an alias; it is tombstoned
	// instead so MAM catch-up does not bridge our own "* text" message back into the room.
	if IsCheogramDMJID(portalID) {
		newID, err := x.sendOut(ctx, outMessage{To: toJID, Type: stanza.ChatMessage, Body: smsEditPrefix + body})
		if err != nil {
			return fail(fmt.Errorf("failed to send XMPP message: %w", err))
		}
		// Tombstone the new id (kept on purpose): our own "* text" message would otherwise come back
		// through MAM catch-up and be bridged into the room as a new message. Side effect: replies,
		// reactions or corrections that name this id are ignored (the tombstone marks it retracted).
		if kv := x.kv(); kv != nil {
			now := strconv.FormatInt(time.Now().Unix(), 10)
			for _, sc := range senderScopes(string(x.loginID), "") {
				kv.Set(ctx, retractedKey(x.loginID, portal.ID, sc, newID), now)
			}
		}
		log.Debug().Str("xmpp_msg_id", newID).Msg("Sent Cheogram edit as a new message")
		return nil
	}

	ref, ok := xmppIDForMessage(target)
	if !ok {
		return fail(errEditNoXMPPRef)
	}
	isMUC := x.portalIsMUC(ctx, portal)
	msgType := stanza.ChatMessage
	var preID string
	if isMUC {
		msgType = stanza.MessageType("groupchat")
		if err := x.ensureInMUC(ctx, portalID); err != nil {
			return fail(err)
		}
		// Register the stanza id before sending (the room's echo of it is ours, see xmpp_muc_echo.go).
		if preID, err = generateXMPPMessageID(); err != nil {
			return fail(err)
		}
		x.mucEcho.register(portalID, preID)
	}
	newID, err := x.sendOut(ctx, outMessage{To: toJID, Type: msgType, ID: preID, Body: body, Replace: ref})
	if err != nil {
		if preID != "" {
			x.mucEcho.discard(portalID, preID)
		}
		return fail(fmt.Errorf("failed to send XMPP correction: %w", err))
	}
	// Record the correction under our own scope: the archive copy of it (MAM catch-up) or a history
	// replay is then recognized as already applied, and a client that corrects THIS correction
	// resolves back to the original.
	if kv := x.kv(); kv != nil {
		for _, sc := range senderScopes(string(x.loginID), "") {
			kv.Set(ctx, correctedKey(x.loginID, networkid.PortalID(portalID), sc, newID), string(target.ID))
		}
	}
	log.Debug().Bool("muc", isMUC).Str("replace_id", ref).Str("xmpp_msg_id", newID).Int("body_len", len(body)).Msg("Sent XMPP correction")
	return nil
}

var _ bridgev2.EditHandlingNetworkAPI = (*xmppClient)(nil)
