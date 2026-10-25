package connector

import (
	"context"
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// refKind says what kind of XMPP reference is being resolved; it decides which message fields may
// match and in which order (anti-spoofing: an occupant chooses its own `id` attribute, so a
// moderation, which always names a room stanza-id, must never be resolved through it).
type refKind int

const (
	// refDM: 1:1 reference (the original's id or origin-id): direct id, then any alias.
	refDM refKind = iota
	// refMUCModeration: a room stanza-id only.
	refMUCModeration
	// refMUCSelf: a room stanza-id first; id/origin-id only when the room assigned none.
	refMUCSelf
	// refReplyDM: target of a reply/reaction in a 1:1 chat: direct id, then origin-id/xmpp id,
	// then the account-archive stanza-id (XEP-0461/0444 name the id the client saw).
	refReplyDM
	// refReplyMUC: target of a reply/reaction in a groupchat: XEP-0461/0444 name the room-assigned
	// stanza-id, so that goes first; then the direct id and the sender-chosen aliases for clients
	// that still reference those. Room-scoped like every lookup here.
	refReplyMUC
	// refCorrectionDM / refCorrectionMUC: target of a XEP-0308 correction. <replace id> names the
	// original's `id` attribute in both chat kinds (not the room stanza-id), so the direct id goes first,
	// then the sender-chosen aliases, then the archive/room stanza-id.
	refCorrectionDM
	refCorrectionMUC
)

// metadata JSON keys (MessageMetadata) that findMessagesByXMPPRef may match. Only these constants are
// ever interpolated into SQL.
const (
	metaKeyStanzaID = "stanza_id"
	metaKeyOriginID = "origin_id"
	metaKeyXMPPID   = "xmpp_message_id"
)

// refStep is one lookup attempt: the direct bridgev2 message-ID lookup, or the metadata aliases.
type refStep struct {
	direct bool
	keys   []string
}

// refLookupPlan returns the ordered lookup steps for a reference kind. Later steps run only when
// the earlier ones found nothing.
func refLookupPlan(kind refKind) []refStep {
	switch kind {
	case refMUCModeration:
		return []refStep{{keys: []string{metaKeyStanzaID}}}
	case refMUCSelf:
		return []refStep{
			{keys: []string{metaKeyStanzaID}},
			{direct: true},
			{keys: []string{metaKeyOriginID, metaKeyXMPPID}},
		}
	case refReplyMUC:
		return []refStep{
			{keys: []string{metaKeyStanzaID}},
			{direct: true},
			{keys: []string{metaKeyOriginID, metaKeyXMPPID}},
		}
	case refCorrectionDM, refCorrectionMUC:
		return []refStep{
			{direct: true},
			{keys: []string{metaKeyOriginID, metaKeyXMPPID}},
			{keys: []string{metaKeyStanzaID}},
		}
	case refReplyDM:
		return []refStep{
			{direct: true},
			{keys: []string{metaKeyOriginID, metaKeyXMPPID}},
			{keys: []string{metaKeyStanzaID}},
		}
	default:
		return []refStep{
			{direct: true},
			{keys: []string{metaKeyStanzaID, metaKeyOriginID, metaKeyXMPPID}},
		}
	}
}

// maxRefAliasCandidates bounds the rows one alias lookup returns.
const maxRefAliasCandidates = 5

// aliasMessageIDs returns the IDs (newest first) of messages in exactly this portal whose metadata
// has ref under one of keys. `->>` with a bare key works on Postgres jsonb and on SQLite >= 3.38.
// Column names are bridgev2's `message` table (pinned to the mautrix version; a test runs the real schema).
func aliasMessageIDs(ctx context.Context, db *database.Database, portal networkid.PortalKey, keys []string, ref string) ([]networkid.MessageID, error) {
	return aliasMessageIDsSince(ctx, db, portal, keys, ref, time.Time{})
}

// aliasMessageIDsSince is aliasMessageIDs restricted to rows with timestamp >= since (bridgev2 stores
// timestamps as Unix nanoseconds); a zero since adds no restriction.
func aliasMessageIDsSince(ctx context.Context, db *database.Database, portal networkid.PortalKey, keys []string, ref string, since time.Time) ([]networkid.MessageID, error) {
	if ref == "" || len(keys) == 0 {
		return nil, nil
	}
	cond := ""
	for i, k := range keys {
		switch k {
		case metaKeyStanzaID, metaKeyOriginID, metaKeyXMPPID:
		default:
			return nil, fmt.Errorf("unsupported metadata key %q", k)
		}
		if i > 0 {
			cond += " OR "
		}
		cond += fmt.Sprintf("metadata->>'%s'=$4", k)
	}
	args := []any{db.BridgeID, portal.ID, portal.Receiver, ref}
	timeCond := ""
	if !since.IsZero() {
		timeCond = " AND timestamp >= $5"
		args = append(args, since.UnixNano())
	}
	query := fmt.Sprintf(`SELECT id FROM message WHERE bridge_id=$1 AND room_id=$2 AND room_receiver=$3%s AND (%s) ORDER BY timestamp DESC LIMIT %d`,
		timeCond, cond, maxRefAliasCandidates)
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []networkid.MessageID
	for rows.Next() {
		var id networkid.MessageID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// findMessagesByXMPPRef resolves an XMPP reference (an id from a retraction, moderation, ...) to the
// bridgev2 message rows of the given portal, following refLookupPlan(kind). Only rows whose Room is
// exactly portal are returned: bridgev2 looks messages up by ID without a room check, so a reference
// to another portal's message must never resolve. The rows of every matching message ID (all parts)
// are returned, first candidate first; callers validate and pick.
func findMessagesByXMPPRef(ctx context.Context, db *database.Database, portal networkid.PortalKey, ref string, kind refKind) ([]*database.Message, error) {
	if ref == "" {
		return nil, nil
	}
	receiver := portal.Receiver
	var out []*database.Message
	seen := map[networkid.MessageID]struct{}{}
	add := func(id networkid.MessageID) error {
		if _, ok := seen[id]; ok {
			return nil
		}
		parts, err := db.Message.GetAllPartsByID(ctx, receiver, id)
		if err != nil {
			return err
		}
		matched := false
		for _, p := range parts {
			if p.Room == portal {
				out = append(out, p)
				matched = true
			}
		}
		if matched {
			seen[id] = struct{}{}
		}
		return nil
	}
	for _, step := range refLookupPlan(kind) {
		if step.direct {
			if err := add(networkid.MessageID(ref)); err != nil {
				return nil, err
			}
		} else {
			ids, err := aliasMessageIDs(ctx, db, portal, step.keys, ref)
			if err != nil {
				return nil, err
			}
			for _, id := range ids {
				if err := add(id); err != nil {
					return nil, err
				}
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	return nil, nil
}
