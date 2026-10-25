package connector

import (
	"go.mau.fi/util/jsontime"
	"maunium.net/go/mautrix/bridgev2/database"
)

// PortalMetadata stores portal-specific metadata for resync tracking.
type PortalMetadata struct {
	// Lazy resync tracking - resync portal if LastSync is older than 24 hours
	LastSync jsontime.Unix `json:"last_sync,omitempty"`
	// MUCPreferredNick is the user-set MUC nickname for this room (muc-nick; used by JoinMUC and as the base of 409 retries).
	MUCPreferredNick string `json:"muc_preferred_nick,omitempty"`
	// MUCAutoNick is an alternative nick the bridge picked after the preferred one got a 409; it is
	// only used while the preferred nick still equals MUCAutoNickBase. Never set by the user.
	MUCAutoNick     string `json:"muc_auto_nick,omitempty"`
	MUCAutoNickBase string `json:"muc_auto_nick_base,omitempty"`
	// MUCLeft is set by an explicit `leave` and cleared by the next join; a left room is not
	// re-joined automatically on connect (muc_autorejoin.go).
	MUCLeft bool `json:"muc_left,omitempty"`
	// MUCInvitePending is set (together with MUCLeft) on a portal created or re-invited from an XMPP
	// MUC invite until the user accepts the Matrix invite (xmpp_invite.go). It is a separate flag so
	// that resyncs keep the user merely invited (getMUCChatInfo) and only accepting joins the room;
	// a plain left room (MUCLeft alone) keeps its user joined in Matrix. Cleared by the next join.
	MUCInvitePending bool `json:"muc_invite_pending,omitempty"`
}

// GhostMetadata stores ghost user metadata for profile refresh tracking.
type GhostMetadata struct {
	ProfileFetchedAt jsontime.UnixMilli `json:"profile_fetched_at"`
	// AvatarHash is the SHA-1 hex (XEP-0153 photo hash) of the last vCard photo applied to
	// this ghost. Used to skip redundant vCard fetches and to detect changes from presence.
	AvatarHash string `json:"avatar_hash,omitempty"`
	// AvatarFetchedAt is when we last attempted a vCard avatar fetch for this ghost. Kept
	// separate from ProfileFetchedAt so pre-existing ghosts (which already have a recent
	// ProfileFetchedAt but no avatar) still get an initial fetch on the next sync.
	AvatarFetchedAt jsontime.UnixMilli `json:"avatar_fetched_at,omitempty"`
}

// MessageMetadata stores message-specific metadata for Jabber messages.
// This gives a DB mapping: stanza_id (XMPPMessageID) ↔ matrix event id (message row/parts).
// This is extensible for future features like backfilling and puppeting.
type MessageMetadata struct {
	// XMPPMessageID is the XMPP stanza `id` attribute. For Matrix→XMPP we set it when sending (the
	// bridgev2 row ID is matrix:$event); for XMPP→Matrix it equals the row ID and is persisted through
	// ConvertedMessagePart.DBMetadata, so reply/reaction lookups can match on it.
	XMPPMessageID string `json:"xmpp_message_id,omitempty"`

	// StanzaID is the XEP-0359 <stanza-id> assigned by a trusted entity: the MUC room (groupchat)
	// or our own account's archive (1:1, equals the MAM <result id>). Never taken from an untrusted `by`.
	StanzaID string `json:"stanza_id,omitempty"`

	// OriginID is the XEP-0359 <origin-id> when present and different from XMPPMessageID.
	OriginID string `json:"origin_id,omitempty"`

	// OccupantID is the XEP-0421 occupant-id of the sender for MUC messages. The room is trusted to
	// have stripped client-supplied values; without that feature it is only sender-chosen.
	OccupantID string `json:"occupant_id,omitempty"`

	// BackfillSource is set for messages fetched via MAM (XEP-0313) backfill/catch-up.
	BackfillSource string `json:"backfill_source,omitempty"` // e.g. "mam"

	// Markable: the inbound message carried XEP-0333 <markable/>; only such messages may be marked displayed.
	Markable bool `json:"markable,omitempty"`

	// Media: the row is a file/media part (inbound OOB/URL media or outbound upload); corrections never
	// turn media into text (Phase D). Unknown (false) for rows stored before this field existed.
	Media bool `json:"media,omitempty"`

	// Future extensibility (not yet used):
	// IsPuppeted bool `json:"is_puppeted,omitempty"` // True if sent via double-puppeting
}

// GetDBMetaTypes returns metadata types for database tables.
func (c *JabberConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		Portal: func() any {
			return &PortalMetadata{}
		},
		Ghost: func() any {
			return &GhostMetadata{}
		},
		Message: func() any {
			return &MessageMetadata{}
		},
		Reaction:  nil,
		UserLogin: func() any { return &JabberLoginMeta{} },
	}
}
