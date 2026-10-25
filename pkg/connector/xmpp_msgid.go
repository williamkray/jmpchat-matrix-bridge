package connector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	genIDPrefix  = "xmp:"     // any bridgev2 id with this prefix is not an XMPP id attribute
	sidIDPrefix  = "xmp:sid:" // + scope + ":" + trusted stanza-id
	hashIDPrefix = "xmp:h:"   // + 32 hex chars, delayed id-less message
	randIDPrefix = "xmp:r:"   // + xid, id-less message without stable data

	// dupWindow bounds how far back bridgedDuplicate looks (offline storage, the 7-day MUC history clamp).
	dupWindow = 30 * 24 * time.Hour
)

// minDelayStamp is the earliest delay stamp that is believed; anything older is treated as no delay.
var minDelayStamp = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// inboundIDs is everything that can name an inbound message. stanzaID must already be trusted
// (stanzaIDBy(scope) or the MAM <result id>); delay must be the trusted, clamped stamp (zero: none).
type inboundIDs struct {
	idAttr, originID, stanzaID string
	scope                      string    // assigner of stanzaID: our bare JID (1:1, carbons, MAM) or the room (groupchat)
	delay                      time.Time // trusted XEP-0203 stamp; zero when none
	senderFull                 string    // as received (occupant JID for groupchat)
	body, oob                  string    // raw parsed body/OOB url, before applyEncryption, group-MMS prefix strip and parseBodyReaction
}

// inboundMessageID picks the bridgev2 message id of an inbound message: the `id` attribute (unchanged
// for backward compatibility), else the origin-id (raw), else the trusted stanza-id scoped by its
// assigner, else a hash of the stable data of a trusted-delay message (so repeated history replays
// map to one row), else a random unique id. Generated ids keep the "xmp:" prefix.
func inboundMessageID(in inboundIDs) networkid.MessageID {
	switch {
	case in.idAttr != "":
		return networkid.MessageID(in.idAttr)
	case in.originID != "":
		return networkid.MessageID(in.originID)
	case in.stanzaID != "" && in.scope != "":
		return networkid.MessageID(sidIDPrefix + strings.ToLower(in.scope) + ":" + in.stanzaID)
	case !in.delay.IsZero():
		h := sha256.New()
		for _, part := range []string{strings.ToLower(in.scope), strings.ToLower(in.senderFull), in.delay.UTC().Format(time.RFC3339Nano), in.body, in.oob} {
			h.Write([]byte(part))
			h.Write([]byte{0})
		}
		return networkid.MessageID(hashIDPrefix + hex.EncodeToString(h.Sum(nil))[:32])
	}
	return networkid.MessageID(randIDPrefix + xid.New().String())
}

// ownScope is the lower-cased bare JID of our account: the assigner of 1:1 stanza-ids.
func (x *xmppClient) ownScope() string {
	return strings.ToLower(bareJID(string(x.loginID)))
}

// trustedDelay returns the earliest stamp of e.Delays whose `from` passes trusted, clamped to now;
// stamps before 2000-01-01 are ignored. Zero when there is none.
func trustedDelay(e messageExtras, now time.Time, trusted func(from string) bool) time.Time {
	var best time.Time
	for _, d := range e.Delays {
		if d.Stamp.Before(minDelayStamp) || !trusted(d.From) {
			continue
		}
		st := d.Stamp
		if st.After(now) {
			st = now
		}
		if best.IsZero() || st.Before(best) {
			best = st
		}
	}
	return best
}

// mucDelayTrust believes a groupchat delay whose `from` is the room (XEP-0045 history stamps).
func mucDelayTrust(room string) func(string) bool {
	room = strings.ToLower(bareJID(room))
	return func(from string) bool {
		return from != "" && strings.ToLower(bareJID(from)) == room
	}
}

// dmDelayTrust believes a 1:1 delay with no `from`, or from our own server (offline storage) or the
// sender's server (or transport). A third-party `from` is ignored.
func dmDelayTrust(ownBare, senderBare string) func(string) bool {
	own, sender := domainOf(ownBare), domainOf(senderBare)
	return func(from string) bool {
		if from == "" {
			return true
		}
		d := domainOf(from)
		return d != "" && (d == own || d == sender)
	}
}

// domainOf returns the lower-cased domain of a JID (the part after '@' of the bare JID; the whole
// bare JID when it has no '@').
func domainOf(j string) string {
	j = strings.ToLower(bareJID(j))
	if i := strings.LastIndex(j, "@"); i >= 0 {
		return j[i+1:]
	}
	return j
}

// mamItemMessageID is the row id of an archived 1:1 message: id attribute, origin-id, archive id
// (scoped to our account), else random (only for a malformed result without any id).
func (x *xmppClient) mamItemMessageID(item mamResultItem) networkid.MessageID {
	return inboundMessageID(inboundIDs{idAttr: item.StanzaID, originID: item.OriginID, stanzaID: item.ArchiveID, scope: x.ownScope()})
}

// dupCandidate describes an inbound message for bridgedDuplicate.
type dupCandidate struct {
	id                                     networkid.MessageID
	idAttr, originID, stanzaID, occupantID string
	sender                                 networkid.UserID
	fromMe                                 bool
	self                                   networkid.UserID
	ts                                     time.Time // event timestamp; window start = min(ts, now) - dupWindow
}

// bridgedDuplicate reports the id of a row of the same portal, with an id different from the
// candidate's, from the same sender, that is the same XMPP message arriving through another path
// (live, carbon, MAM, MUC history): trusted stanza-id against stanza_id, then the origin-id against
// origin_id/xmpp_message_id and as a direct row id, then the `id` attribute against
// origin_id/xmpp_message_id (Matrix-originated rows). Every step is sender-restricted, so a sender can
// only suppress its own messages. "" when none. DB access: portal event loop or goroutines only.
func bridgedDuplicate(ctx context.Context, db *database.Database, portal networkid.PortalKey, c dupCandidate) (networkid.MessageID, error) {
	if db == nil {
		return "", nil
	}
	now := time.Now()
	ts := c.ts
	if ts.IsZero() || ts.After(now) {
		ts = now
	}
	since := ts.Add(-dupWindow)
	check := func(keys []string, ref string) (networkid.MessageID, error) {
		if ref == "" {
			return "", nil
		}
		ids, err := aliasMessageIDsSince(ctx, db, portal, keys, ref, since)
		if err != nil {
			return "", err
		}
		for _, id := range ids {
			if id == c.id {
				continue
			}
			if ok, err := dupRowMatches(ctx, db, portal, id, c); ok || err != nil {
				return id, err
			}
		}
		return "", nil
	}
	if id, err := check([]string{metaKeyStanzaID}, c.stanzaID); id != "" || err != nil {
		return id, err
	}
	if id, err := check([]string{metaKeyOriginID, metaKeyXMPPID}, c.originID); id != "" || err != nil {
		return id, err
	}
	if c.originID != "" && networkid.MessageID(c.originID) != c.id {
		if ok, err := dupRowMatches(ctx, db, portal, networkid.MessageID(c.originID), c); ok || err != nil {
			if err != nil {
				return "", err
			}
			return networkid.MessageID(c.originID), nil
		}
	}
	if c.idAttr != c.originID {
		return check([]string{metaKeyOriginID, metaKeyXMPPID}, c.idAttr)
	}
	return "", nil
}

// dupRowMatches reports whether some part of message id lies in portal, is not older than the window
// and was sent by the candidate's sender (dupSameSender).
func dupRowMatches(ctx context.Context, db *database.Database, portal networkid.PortalKey, id networkid.MessageID, c dupCandidate) (bool, error) {
	parts, err := db.Message.GetAllPartsByID(ctx, portal.Receiver, id)
	if err != nil {
		return false, err
	}
	for _, p := range parts {
		if p.Room == portal && dupSameSender(p, c) {
			return true, nil
		}
	}
	return false, nil
}

// dupSameSender reports whether row was sent by the candidate's sender: the same sender id
// (case-insensitive); for a from-me candidate any row of our own account (any resource); or the same
// occupant-id (a MUC nick change between copies).
func dupSameSender(row *database.Message, c dupCandidate) bool {
	if row == nil {
		return false
	}
	if strings.EqualFold(string(row.SenderID), string(c.sender)) {
		return true
	}
	if c.fromMe && c.self != "" && strings.EqualFold(bareJID(string(row.SenderID)), bareJID(string(c.self))) {
		return true
	}
	if c.occupantID != "" {
		if meta, ok := row.Metadata.(*MessageMetadata); ok && meta != nil && meta.OccupantID == c.occupantID {
			return true
		}
	}
	return false
}

// isBridgedDuplicate wraps bridgedDuplicate for ConvertMessage. A lookup error is logged and treated
// as "not a duplicate" (a duplicate is better than a lost message).
func (m *jabberRemoteMessage) isBridgedDuplicate(ctx context.Context) bool {
	if m.x == nil || m.x.conn == nil || m.x.conn.br == nil {
		return false
	}
	dup, err := bridgedDuplicate(ctx, m.x.conn.br.DB, m.portalKey, dupCandidate{
		id: m.id, idAttr: m.idAttr, originID: m.originID, stanzaID: m.stanzaID, occupantID: m.occupantID,
		sender: m.sender.Sender, fromMe: m.sender.IsFromMe, self: networkid.UserID(m.x.loginID), ts: m.GetTimestamp(),
	})
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Cross-path duplicate lookup failed; bridging the message")
		return false
	}
	return dup != ""
}

// mamItemAlreadyBridged reports whether an archived 1:1 message is already in the portal: a row with
// its id, or the same message bridged under another id (bridgedDuplicate). Errors are logged at debug
// level and treated as "not bridged". Runs in the catch-up/backfill goroutines.
func (x *xmppClient) mamItemAlreadyBridged(ctx context.Context, portalKey networkid.PortalKey, item mamResultItem, msgID networkid.MessageID, sender bridgev2.EventSender) bool {
	if x.conn == nil || x.conn.br == nil {
		return false
	}
	db := x.conn.br.DB
	if existing, err := db.Message.GetFirstPartByID(ctx, x.loginID, msgID); err != nil {
		x.logFor(ctx).Debug().Err(err).Msg("MAM duplicate check: row lookup failed")
	} else if existing != nil {
		return true
	}
	dup, err := bridgedDuplicate(ctx, db, portalKey, dupCandidate{
		id: msgID, idAttr: item.StanzaID, originID: item.OriginID, stanzaID: item.ArchiveID,
		sender: sender.Sender, fromMe: sender.IsFromMe || strings.EqualFold(bareJID(item.From), x.ownScope()), self: networkid.UserID(x.loginID), ts: item.Timestamp,
	})
	if err != nil {
		x.logFor(ctx).Debug().Err(err).Msg("MAM duplicate check: lookup failed")
		return false
	}
	return dup != ""
}
