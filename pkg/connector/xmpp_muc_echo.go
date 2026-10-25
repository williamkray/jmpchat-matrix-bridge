package connector

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmpp/jid"
)

// MUC echo rendezvous. A MUC retraction must name the ROOM-assigned stanza-id of the original, which
// we only learn from the room's echo of our own message. The echo (Serve goroutine) and the
// bridgev2 row insert (portal event loop, signalled through MatrixMessageResponse.PostSave) happen in
// either order, so both sides meet in mucEchoTable under one lock: whoever comes second finishes the
// handoff. No sleeps or DB polling. A Matrix redaction that arrives before the echo waits on the
// entry's done channel (bounded, see mucEchoWait).
//
// Matching is by exact id: the origin-id (preferred, rooms keep it) or the `id` attribute of a
// message WE sent and registered before sending. Issue #11 (messages from our own nick sent by other
// clients are dropped as self-echo) is fixed on top of this: only a message that matches a registered
// id is dropped as our echo; any other message from our own occupant JID was sent by another client
// of the user and is bridged as sent by the user (mucMessageSender). Every bridge MUC send path
// (text/media, reactions, retractions) must register its stanza id before sending.

const (
	// mucEchoTTL is how long an entry is kept (it also serves redactions that come after the echo).
	mucEchoTTL = 5 * time.Minute
	// mucEchoWait bounds how long a redaction waits for a pending echo.
	mucEchoWait = 3 * time.Second
)

type mucEchoKey struct{ room, id string }

func newMUCEchoKey(room, id string) mucEchoKey {
	return mucEchoKey{room: strings.ToLower(room), id: id}
}

type mucEchoEntry struct {
	created    time.Time
	done       chan struct{} // closed when the echo was seen
	echoed     bool
	stanzaID   string // room-assigned; "" when the room stamps none
	occupantID string
	saved      bool // PostSave ran and the echo was not there yet
	mxid       id.EventID
}

// mucEchoTable is usable as a zero value.
type mucEchoTable struct {
	mu      sync.Mutex
	entries map[mucEchoKey]*mucEchoEntry
	now     func() time.Time // tests only
}

func (t *mucEchoTable) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// register creates the entry for a message about to be sent (must happen BEFORE the send so that a
// fast echo cannot be missed). Expired entries are swept here.
func (t *mucEchoTable) register(room, msgID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	if t.entries == nil {
		t.entries = map[mucEchoKey]*mucEchoEntry{}
	}
	for k, e := range t.entries {
		if now.Sub(e.created) > mucEchoTTL {
			delete(t.entries, k)
		}
	}
	k := newMUCEchoKey(room, msgID)
	if _, ok := t.entries[k]; !ok {
		t.entries[k] = &mucEchoEntry{created: now, done: make(chan struct{})}
	}
}

// discard drops an entry (the send failed).
func (t *mucEchoTable) discard(room, msgID string) {
	t.mu.Lock()
	delete(t.entries, newMUCEchoKey(room, msgID))
	t.mu.Unlock()
}

// echoResult is what the echo side learns.
type echoResult struct {
	matched bool
	persist bool       // the row is already saved: write the ids to it
	mxid    id.EventID // row to update when persist
}

// matchEcho records the echo of a registered message. ids are the candidates the echo carries
// (origin-id first, then the `id` attribute). stanzaID must already be the room's trusted stanza-id.
// A second echo of the same message is matched but changes nothing.
func (t *mucEchoTable) matchEcho(room string, ids []string, stanzaID, occupantID string) echoResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	for _, cand := range ids {
		if cand == "" {
			continue
		}
		e, ok := t.entries[newMUCEchoKey(room, cand)]
		if !ok || now.Sub(e.created) > mucEchoTTL {
			continue
		}
		if e.echoed {
			return echoResult{matched: true}
		}
		e.echoed = true
		e.stanzaID = stanzaID
		e.occupantID = occupantID
		close(e.done)
		res := echoResult{matched: true}
		if e.saved {
			res.persist = true
			res.mxid = e.mxid
		}
		return res
	}
	return echoResult{}
}

// postSave is the row-insert side. When the echo already arrived it returns its ids (the caller
// writes them to the saved row); otherwise it remembers the row for the echo side.
func (t *mucEchoTable) postSave(room, msgID string, mxid id.EventID) (stanzaID, occupantID string, haveEcho bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[newMUCEchoKey(room, msgID)]
	if !ok {
		return "", "", false
	}
	if e.echoed {
		return e.stanzaID, e.occupantID, true
	}
	e.saved = true
	e.mxid = mxid
	return "", "", false
}

// wait returns the room stanza-id recorded for the message. found=false means no entry exists
// (expired or never registered). If the echo is still pending it waits up to timeout (or ctx);
// timedOut=true when it did not arrive. A confirmed echo without a stanza-id returns "" with found.
func (t *mucEchoTable) wait(ctx context.Context, room, msgID string, timeout time.Duration) (stanzaID string, found, timedOut bool) {
	t.mu.Lock()
	e, ok := t.entries[newMUCEchoKey(room, msgID)]
	t.mu.Unlock()
	if !ok {
		return "", false, false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-e.done:
	case <-timer.C:
		return "", true, true
	case <-ctx.Done():
		return "", true, true
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return e.stanzaID, true, false
}

// applyEchoIDs writes the echo's ids into the row metadata; false when nothing changed.
func applyEchoIDs(meta *MessageMetadata, stanzaID, occupantID string) bool {
	if meta == nil {
		return false
	}
	changed := false
	if stanzaID != "" && meta.StanzaID != stanzaID {
		meta.StanzaID = stanzaID
		changed = true
	}
	if occupantID != "" && meta.OccupantID != occupantID {
		meta.OccupantID = occupantID
		changed = true
	}
	return changed
}

// mucEchoPostSave returns the PostSave callback for a registered MUC message (portal event loop;
// DB access allowed).
func (x *xmppClient) mucEchoPostSave(room, msgID string) func(context.Context, *database.Message) {
	return func(ctx context.Context, saved *database.Message) {
		stanzaID, occ, have := x.mucEcho.postSave(room, msgID, saved.MXID)
		if !have {
			return
		}
		meta, _ := saved.Metadata.(*MessageMetadata)
		if !applyEchoIDs(meta, stanzaID, occ) {
			return
		}
		if err := x.conn.br.DB.Message.Update(ctx, saved); err != nil {
			x.logFor(ctx).Warn().Err(err).Msg("Failed to store MUC echo ids on sent message")
		}
	}
}

// isOwnMUCOccupant reports whether occupantFull is our own occupant JID in the room (memory only).
func (x *xmppClient) isOwnMUCOccupant(room, occupantFull string) bool {
	ours := x.getMUCOurOccupantJID(room)
	return ours != "" && occupantFull == ours
}

// noteOwnMUCNickChange follows a nick change of our own occupant (unavailable + 303 from oldOccupant).
// It is ours when oldOccupant is the occupant JID we recorded for the room, or the presence carries the
// self-presence status 110. Then the recorded occupant JID becomes room/newNick under its lock, which is
// all the echo matching (handleOwnMUCEcho, mucMessageSender, leaveMUC) reads. A rename of anybody else
// changes nothing here (the avatar table drops the old occupant record). Serve goroutine, memory only.
func (x *xmppClient) noteOwnMUCNickChange(roomBare, oldOccupant string, info presenceInfo) bool {
	roomJID, err := jid.Parse(roomBare)
	if err != nil {
		return false
	}
	newJID, err := roomJID.WithResource(info.newNick)
	if err != nil {
		return false
	}
	x.mucOurOccupantJIDMu.Lock()
	ours := x.mucOurOccupantJID[roomBare]
	if !(ours != "" && ours == oldOccupant) && !info.hasStatus("110") {
		x.mucOurOccupantJIDMu.Unlock()
		return false
	}
	if x.mucOurOccupantJID == nil {
		x.mucOurOccupantJID = make(map[string]string)
	}
	// The JIDs we held until now stay recognizable (echo/replay matching of messages sent just before
	// the rename), see mucPastOwn.
	now := time.Now()
	if oldOccupant != newJID.String() {
		x.noteMUCPastOwnLocked(roomBare, oldOccupant, now)
	}
	if ours != "" && ours != oldOccupant && ours != newJID.String() {
		x.noteMUCPastOwnLocked(roomBare, ours, now)
	}
	x.mucOurOccupantJID[roomBare] = newJID.String()
	x.mucOurOccupantJIDMu.Unlock()
	x.log.Info().Str("room", roomBare).Str("old_nick", resourceOf(oldOccupant)).Str("new_nick", info.newNick).Msg("MUC own nick changed, following it")
	return true
}

// resourceOf returns the resource part of a JID string ("" if none).
func resourceOf(j string) string {
	if i := strings.IndexByte(j, '/'); i >= 0 {
		return j[i+1:]
	}
	return ""
}

// mucMessageSender is the bridged sender of a groupchat message: the user themselves (is_from_me,
// double puppet, like 1:1 carbons) when it comes from our own occupant JID, else the occupant's ghost.
// Only called for messages that are not registered echoes (issue #11).
func (x *xmppClient) mucMessageSender(room, occupantFull string) bridgev2.EventSender {
	if x.isOwnMUCOccupant(room, occupantFull) {
		return x.makeEventSender(string(x.loginID))
	}
	return x.makeEventSender(occupantFull)
}

// handleOwnMUCEcho is called on the Serve goroutine for every inbound groupchat message (memory
// only). It returns true when the message is the echo of a message this bridge sent, which must not
// be bridged back. The DB write (echo arrived after the row was saved) runs in a goroutine without a
// span.
func (x *xmppClient) handleOwnMUCEcho(room, occupantFull, idAttr string, ids messageIDs) bool {
	// An echo comes from our own occupant JID; when we do not know it (yet), the registered random id
	// is the only evidence. A known, different occupant is never ours.
	// An echo of a message sent just before our own rename comes from the old occupant JID.
	if ours := x.getMUCOurOccupantJID(room); ours != "" && occupantFull != ours && !x.wasOwnMUCOccupantForEcho(room, occupantFull, time.Now()) {
		return false
	}
	res := x.mucEcho.matchEcho(room, []string{ids.OriginID, idAttr}, ids.stanzaIDBy(room), ids.OccupantID)
	if !res.matched {
		return false
	}
	if res.persist {
		stanzaID, occ := ids.stanzaIDBy(room), ids.OccupantID
		mxid := res.mxid
		go x.persistEchoIDs(mxid, stanzaID, occ)
	}
	x.log.Debug().Str("room", room).Msg("Recognized MUC echo of our own sent message")
	return true
}

// persistEchoIDs re-reads the row by event ID (so a stale struct never clobbers other columns) and
// stores the echo ids.
func (x *xmppClient) persistEchoIDs(mxid id.EventID, stanzaID, occupantID string) {
	ctx := x.log.WithContext(context.Background())
	if x.conn == nil || x.conn.br == nil || mxid == "" {
		return
	}
	row, err := x.conn.br.DB.Message.GetPartByMXID(ctx, mxid)
	if err != nil || row == nil {
		x.log.Warn().Err(err).Msg("MUC echo ids: sent message row not found")
		return
	}
	meta, _ := row.Metadata.(*MessageMetadata)
	if !applyEchoIDs(meta, stanzaID, occupantID) {
		return
	}
	if err := x.conn.br.DB.Message.Update(ctx, row); err != nil {
		x.log.Warn().Err(err).Msg("Failed to store MUC echo ids on sent message")
	}
}

// ownMUCReplayExists reports whether a groupchat message from our own occupant JID (one that is NOT a
// registered echo, see handleOwnMUCEcho) is a replay of a message already bridged in this portal and
// sent by the user: MUC history on (re)join or any later re-delivery of a message the BRIDGE sent
// after its echo-table entry expired. Those rows are Matrix-originated (`matrix:` id with the XMPP
// ids in metadata), so bridgev2's duplicate check by message id does not catch them.
//
// Matching, room-scoped and restricted to rows whose sender is the user (so another occupant can
// never make one of our messages disappear): the trusted room stanza-id against stanza_id, then the
// origin-id and the `id` attribute against origin_id / xmpp_message_id, plus the origin-id as a
// direct row id (a message from another client of the user bridged earlier, whose id attribute the
// room rewrote). A genuinely new message from another client carries fresh ids and matches nothing.
// DB access: portal event loop only, never on the Serve goroutine.
func ownMUCReplayExists(ctx context.Context, db *database.Database, portal networkid.PortalKey, self networkid.UserID, stanzaID, originID, idAttr string) (bool, error) {
	if db == nil {
		return false, nil
	}
	ownRow := func(id networkid.MessageID) (bool, error) {
		parts, err := db.Message.GetAllPartsByID(ctx, portal.Receiver, id)
		if err != nil {
			return false, err
		}
		for _, p := range parts {
			if p.Room == portal && p.SenderID == self {
				return true, nil
			}
		}
		return false, nil
	}
	check := func(keys []string, ref string) (bool, error) {
		if ref == "" {
			return false, nil
		}
		ids, err := aliasMessageIDs(ctx, db, portal, keys, ref)
		if err != nil {
			return false, err
		}
		for _, id := range ids {
			if ok, err := ownRow(id); ok || err != nil {
				return ok, err
			}
		}
		return false, nil
	}
	if ok, err := check([]string{metaKeyStanzaID}, stanzaID); ok || err != nil {
		return ok, err
	}
	for _, ref := range []string{originID, idAttr} {
		if ok, err := check([]string{metaKeyOriginID, metaKeyXMPPID}, ref); ok || err != nil {
			return ok, err
		}
	}
	if originID != "" && originID != idAttr {
		return ownRow(networkid.MessageID(originID))
	}
	return false, nil
}

// isOwnMUCReplay is ownMUCReplayExists for an inbound from-me groupchat message. A lookup error is
// logged and treated as "not a replay" (a duplicate is better than a lost message).
func (m *jabberRemoteMessage) isOwnMUCReplay(ctx context.Context) bool {
	if !m.groupchat || !(m.sender.IsFromMe || m.pastOwnOccupant) || m.x == nil || m.x.conn == nil || m.x.conn.br == nil {
		return false
	}
	dup, err := ownMUCReplayExists(ctx, m.x.conn.br.DB, m.portalKey, networkid.UserID(m.x.loginID), m.stanzaID, m.originID, string(m.id))
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("Own MUC replay lookup failed; bridging the message")
		return false
	}
	return dup
}
