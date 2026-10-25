// MUC occupant avatars (XMPP -> Matrix only), see the avatars section of the README.
//
// Signal: the XEP-0153 vcard-temp:x:update hash in an occupant's MUC presence. Presence handling
// on the mellium Serve goroutine only updates an in-memory occupant table and does a non-blocking
// enqueue (no DB, no ghost lookup, no IQ, no HTTP, no spans). A single worker goroutine per session
// does all ghost lookups, vcard-temp IQs and ghost updates. An occupant is only considered once a
// ghost for it was seen this session (it spoke), so a join storm costs table entries only.
// The worker is the sole writer of MUC occupant avatars; getUserInfo keeps returning a nil Avatar
// for MUC occupants (a stale Avatar from the Serve side could revert or corrupt the applied one).
//
// Contact rule (non-anonymous rooms): XMPP has no per-room avatars, the XEP-0153 hash in MUC
// presence mirrors the account avatar, and rooms often keep advertising an old hash because clients
// do not resend room presence when the account avatar changes. So when the room reports the
// occupant's real JID and we hold a 1:1 contact ghost for that bare JID whose avatar is known (see
// classifyContactGhost), the contact ghost's avatar is authoritative for the occupant: the worker
// applies the contact's already-uploaded MXC (or removes the avatar when the contact is known to
// have none), with no IQ and no upload, and skips the room-hash fetch. The room-hash path stays
// the fallback when there is no real JID or no contact ghost with a known avatar. When the room
// advertises a state that differs from the contact's, the contact may be the stale side: the worker
// then asks the 1:1 path to refresh the contact (doAvatarRefresh, which propagates a change back
// here), at most once per contact per mucContactRefreshAge and once per advertised state per
// mucAvatarFailRetry, and only when the contact was fetched more than mucContactRefreshAge ago.
// Propagation (propagateContact) covers changes while the occupant is known; the contact rule
// covers occupants first seen after the change (e.g. joined after a restart).

package connector

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/stanza"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

const (
	mucOccupantCap       = 20000            // global cap on occupant table entries
	mucHistoryCap        = 1024             // cap on history-only entries (ghost seen, no presence seen)
	mucAvatarMinInterval = 60 * time.Second // minimum time between jobs doing work for one occupant
	mucAvatarMaxPending  = 1024             // cap on queued jobs
	mucAvatarMXCCap      = 512              // cap on the advertised/real hash -> MXC cache
	mucAvatarPace        = 250 * time.Millisecond
	mucAvatarWaitPause   = 60 * time.Second
	mucAvatarFailRetry   = 6 * time.Hour
	mucAvatarRoomErrMax  = 5
	mucAvatarStatsPeriod = 5 * time.Minute  // info-level stats summary: at most this often, only when changed
	mucAvatarFailClasses = 32               // cap on distinct failure classes counted for the stats line
	mucContactRefreshAge = 10 * time.Minute // contact rule: min age of a contact fetch before a room-triggered refresh
	mucContactReqCap     = 1024             // cap on the room-triggered contact refresh record
)

// mucOccupant is the per-occupant avatar record (key: occupant full JID room@muc/nick).
type mucOccupant struct {
	room     string
	photo    photoKind
	hash     string    // advertised (presence) hash, lowercased
	realJID  string    // bare, optional, never logged at info
	hasGhost bool      // a ghost for this occupant was seen this session
	live     bool      // a presence for this occupant was seen this session (else history-only)
	lastWork time.Time // when the worker last started real work (fetch/apply/remove) for it
	doneHash string    // advertised state ("none" or hash) last fully handled
	failHash string    // advertised state whose fetch failed
	failAt   time.Time
	prop     *mucProp // pending propagation from the 1:1 path (latest wins)
	// contactTried: a job with the real JID was dequeued this session. Lets an occupant whose
	// presence carries no usable photo element get one contact-rule job (see shouldEnqueueLocked).
	contactTried bool
}

// mucProp is an avatar the 1:1 path just applied for the occupant's real JID: remove, or apply the
// already-uploaded mxc (no IQ, no upload).
type mucProp struct {
	remove bool
	mxc    mucMXC
}

// adv is the advertised state: "none", the hash, or "" when unknown.
func (o *mucOccupant) adv() string {
	switch o.photo {
	case photoNone:
		return "none"
	case photoHash:
		return o.hash
	}
	return ""
}

// mucOccSnapshot is a copy of an occupant record taken under the lock for the worker.
type mucOccSnapshot struct {
	room    string
	photo   photoKind
	hash    string
	realJID string
	prop    *mucProp
}

func (s mucOccSnapshot) adv() string { return (&mucOccupant{photo: s.photo, hash: s.hash}).adv() }

// mucMXC is a pre-uploaded avatar: the SHA-1 of the photo we hashed, and the MXC/SHA-256 the
// bridge produced when it first uploaded it.
type mucMXC struct {
	realHash string
	mxc      string
	sha256   [32]byte
}

type mucAvatarStats struct {
	enqueued, coalesced, droppedFull, fetched, applied, removed, failed, rejected, skippedDisabled int
}

// avatarGhost is the slice of *bridgev2.Ghost the worker uses (fakeable in tests).
type avatarGhost interface {
	UpdateInfo(ctx context.Context, info *bridgev2.UserInfo)
	StoredAvatarHash() string // GhostMetadata.AvatarHash (SHA-1 hex of the last applied photo)
	AvatarApplied() bool      // ghost.AvatarSet: the avatar is set on Matrix
}

type realAvatarGhost struct{ g *bridgev2.Ghost }

func (r realAvatarGhost) UpdateInfo(ctx context.Context, info *bridgev2.UserInfo) {
	r.g.UpdateInfo(ctx, info)
}

func (r realAvatarGhost) StoredAvatarHash() string {
	if m, ok := r.g.Metadata.(*GhostMetadata); ok && m != nil {
		return m.AvatarHash
	}
	return ""
}

func (r realAvatarGhost) AvatarApplied() bool { return r.g.AvatarSet }

// mucAvatarState holds the occupant table, the job queue, and the worker's state. The zero value
// is usable; reset starts a fresh session.
type mucAvatarState struct {
	mu             sync.Mutex
	occ            map[string]*mucOccupant
	queue          []string
	pending        map[string]struct{}
	wake           chan struct{}
	roomDisabled   map[string]bool
	roomCancelErrs map[string]int
	mxcByHash      map[string]mucMXC // kept across sessions
	pausedUntil    time.Time
	capLogged      bool
	histOrder      []string // keys of history-only entries, oldest first (may hold stale keys)
	histN          int      // number of live table entries that are history-only
	stats          mucAvatarStats
	failBy         map[string]int // failure class ("cancel/item-not-found", "timeout", ...) -> count
	statsVer       uint64         // bumped on every stats change
	lastStatsVer   uint64
	lastStatsAt    time.Time
	contactReq     map[string]mucContactReq // room-triggered contact refreshes, kept across sessions

	// Seams (defaults installed by the xmppClient; tests set their own).
	now      func() time.Time
	fetch    func(ctx context.Context, to jid.JID, spanPeer string) (data []byte, sha1Hex string, err error)
	getGhost func(ctx context.Context, occupant string) (avatarGhost, error)
	pace     func(ctx context.Context) error
	paceC    <-chan time.Time // current session's pacing ticker (production pace), guarded by mu
	sleep    func(ctx context.Context, d time.Duration) error
	logger   func(ctx context.Context) *zerolog.Logger
	// getContact loads the 1:1 contact ghost for a bare real JID (no creation, no I/O besides the
	// DB). nil disables the contact rule.
	getContact func(ctx context.Context, realBare string) (contactAvatar, error)
	// refreshContact queues a 1:1 avatar refresh for the contact ghost id (never blocks).
	refreshContact func(contactID string)
}

// contactAvatarState is what a 1:1 contact ghost says about the account avatar.
type contactAvatarState int

const (
	contactUnknown contactAvatarState = iota // no ghost, never fetched, or no proven MXC: use the room path
	contactHas                               // avatar with a proven MXC
	contactNone                              // fetched, known to have no avatar
)

// contactAvatar is a read-only snapshot of a 1:1 contact ghost's avatar.
type contactAvatar struct {
	state     contactAvatarState
	id        string // ghost id (bare JID) as stored
	mxc       mucMXC // contactHas only
	fetchedAt time.Time
}

// adv is the contact's state in the advertised-state vocabulary ("none" or the SHA-1).
func (c contactAvatar) adv() string {
	if c.state == contactHas {
		return c.mxc.realHash
	}
	return "none"
}

// mucContactReq records the last room-triggered refresh of a contact.
type mucContactReq struct {
	adv string
	at  time.Time
}

// classifyContactGhost decides what a contact ghost row says. contactHas needs a proven MXC: the
// stored SHA-1 (computed by us from the fetched bytes) matches the ghost's AvatarID, the MXC is set
// on Matrix (AvatarSet; a failed upload or profile set leaves it false) and its SHA-256 is known.
// contactNone needs a completed fetch (AvatarFetchedAt) with no stored hash and no avatar on the
// ghost, set on Matrix. A rejected photo of a contact that never had an avatar looks the same; the
// occupant then shows no avatar, as it would after the room-path fetch rejected the same photo.
func classifyContactGhost(ghostID string, avatarID networkid.AvatarID, avatarMXC id.ContentURIString, avatarSHA [32]byte, avatarSet bool, meta *GhostMetadata) contactAvatar {
	c := contactAvatar{id: ghostID}
	if meta == nil || meta.AvatarFetchedAt.Time.IsZero() || !avatarSet {
		return c
	}
	c.fetchedAt = meta.AvatarFetchedAt.Time
	switch {
	case meta.AvatarHash != "":
		if avatarMXC != "" && avatarSHA != ([32]byte{}) && avatarID == avatarIDFromHash(meta.AvatarHash) {
			c.state = contactHas
			c.mxc = mucMXC{realHash: meta.AvatarHash, mxc: string(avatarMXC), sha256: avatarSHA}
		}
	case avatarID == "" && avatarMXC == "":
		c.state = contactNone
	}
	return c
}

func (s *mucAvatarState) nowT() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *mucAvatarState) log(ctx context.Context) *zerolog.Logger {
	if s.logger != nil {
		return s.logger(ctx)
	}
	nop := zerolog.Nop()
	return &nop
}

// initLocked lazily creates the maps. Requires s.mu.
func (s *mucAvatarState) initLocked() {
	if s.occ == nil {
		s.occ = make(map[string]*mucOccupant)
		s.pending = make(map[string]struct{})
		s.roomDisabled = make(map[string]bool)
		s.roomCancelErrs = make(map[string]int)
	}
	if s.wake == nil {
		s.wake = make(chan struct{}, 1)
	}
	if s.mxcByHash == nil {
		s.mxcByHash = make(map[string]mucMXC)
	}
	if s.contactReq == nil {
		s.contactReq = make(map[string]mucContactReq)
	}
	if s.failBy == nil {
		s.failBy = make(map[string]int)
	}
}

// reset starts a fresh session: a rejoin re-sends every presence, so the table, queue and
// per-room state are dropped. The MXC cache survives (MXC URIs stay valid).
func (s *mucAvatarState) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.occ = nil
	s.queue = nil
	s.pending = nil
	s.roomDisabled = nil
	s.roomCancelErrs = nil
	s.pausedUntil = time.Time{}
	s.capLogged = false
	s.histOrder = nil
	s.histN = 0
	s.initLocked()
}

// shouldEnqueueLocked is the single enqueue condition (see the plan, section 2.2).
func (s *mucAvatarState) shouldEnqueueLocked(rec *mucOccupant) bool {
	if !rec.hasGhost || s.roomDisabled[rec.room] {
		return false
	}
	if rec.photo == photoUnknown {
		// Nothing advertised: only the contact rule can act (no IQ), once per session.
		return rec.realJID != "" && !rec.contactTried && s.getContact != nil
	}
	adv := rec.adv()
	if adv == rec.doneHash {
		return false
	}
	if !rec.lastWork.IsZero() && s.nowT().Sub(rec.lastWork) < mucAvatarMinInterval {
		return false // a later presence or message after the interval re-triggers
	}
	if adv == rec.failHash && s.nowT().Sub(rec.failAt) < mucAvatarFailRetry {
		return false
	}
	return true
}

// enqueueLocked adds key to the queue (coalescing). Never blocks.
func (s *mucAvatarState) enqueueLocked(key string) {
	if _, ok := s.pending[key]; ok {
		s.stats.coalesced++
		s.statsVer++
		return
	}
	if len(s.pending) >= mucAvatarMaxPending {
		s.stats.droppedFull++
		s.statsVer++
		return
	}
	if len(s.queue) >= 2*mucAvatarMaxPending { // compact stale keys left by deletions
		q := s.queue[:0]
		for _, k := range s.queue {
			if _, ok := s.pending[k]; ok {
				q = append(q, k)
			}
		}
		s.queue = q
	}
	s.pending[key] = struct{}{}
	s.queue = append(s.queue, key)
	s.stats.enqueued++
	s.statsVer++
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// deleteOccLocked removes an occupant record and its pending job. Requires s.mu.
func (s *mucAvatarState) deleteOccLocked(key string) {
	if rec := s.occ[key]; rec != nil && !rec.live {
		s.histN--
	}
	delete(s.occ, key)
	delete(s.pending, key)
}

// evictHistoryLocked drops the oldest history-only entry (a sender that spoke without a presence
// we saw, e.g. MAM/history replay). It reports whether one was dropped. Requires s.mu.
func (s *mucAvatarState) evictHistoryLocked() bool {
	for len(s.histOrder) > 0 {
		k := s.histOrder[0]
		s.histOrder = s.histOrder[1:]
		if rec := s.occ[k]; rec != nil && !rec.live {
			s.deleteOccLocked(k)
			return true
		}
	}
	return false
}

// notePresence records an occupant's MUC presence. unavailable covers leave, kick, ban and
// nick change (303): the record and its pending job are dropped. own is our own occupant.
// Returns true when the occupant table is full and the entry was ignored for the first time.
func (s *mucAvatarState) notePresence(occ, room string, info presenceInfo, unavailable, own bool) (capReached bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	if own {
		// Our own leave/kick drops the whole room; our own rename (303) keeps everyone else's records.
		if unavailable && !info.hasStatus("303") {
			s.clearRoomLocked(room)
		}
		return false
	}
	if unavailable {
		s.deleteOccLocked(occ)
		return false
	}
	rec := s.occ[occ]
	if rec == nil {
		// Live occupants win over history-only entries when the table is full.
		if len(s.occ) >= mucOccupantCap && !s.evictHistoryLocked() {
			first := !s.capLogged
			s.capLogged = true
			return first
		}
		rec = &mucOccupant{room: room, live: true}
		s.occ[occ] = rec
	} else if !rec.live {
		rec.live = true
		s.histN--
	}
	// A presence without a usable photo element (unknown) says nothing: keep what we knew.
	if info.photo != photoUnknown {
		rec.photo = info.photo
		rec.hash = info.hash
	}
	if info.itemJID != "" {
		rec.realJID = info.itemJID
	}
	if s.shouldEnqueueLocked(rec) {
		s.enqueueLocked(occ)
	}
	return false
}

// noteGhost records that a ghost for the occupant was seen this session (it spoke) and, if that
// flips the flag, applies the enqueue condition. Memory only.
func (s *mucAvatarState) noteGhost(occ string) (capReached bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	rec := s.occ[occ]
	if rec == nil {
		// History-only entries are bounded: evict the oldest one instead of growing.
		if s.histN >= mucHistoryCap {
			s.evictHistoryLocked()
		}
		if len(s.occ) >= mucOccupantCap && !s.evictHistoryLocked() {
			first := !s.capLogged
			s.capLogged = true
			return first
		}
		rec = &mucOccupant{room: bareOf(occ)}
		s.occ[occ] = rec
		s.histN++
		if len(s.histOrder) >= 4*mucHistoryCap { // drop stale keys
			q := s.histOrder[:0]
			for _, k := range s.histOrder {
				if r := s.occ[k]; r != nil && !r.live {
					q = append(q, k)
				}
			}
			s.histOrder = q
		}
		s.histOrder = append(s.histOrder, occ)
	}
	if rec.hasGhost {
		return false
	}
	rec.hasGhost = true
	if s.shouldEnqueueLocked(rec) {
		s.enqueueLocked(occ)
	}
	return false
}

// clearRoom drops every occupant record and pending job of a room (LeaveMUC).
func (s *mucAvatarState) clearRoom(room string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clearRoomLocked(room)
}

func (s *mucAvatarState) clearRoomLocked(room string) {
	for k, rec := range s.occ {
		if rec.room == room {
			s.deleteOccLocked(k)
		}
	}
}

// dequeue pops the next live job and snapshots its record; ok is false when the queue is empty.
func (s *mucAvatarState) dequeue() (key string, snap mucOccSnapshot, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.queue) > 0 {
		k := s.queue[0]
		s.queue = s.queue[1:]
		if _, live := s.pending[k]; !live {
			continue // dropped (unavailable) or duplicate
		}
		delete(s.pending, k)
		rec := s.occ[k]
		if rec == nil {
			continue
		}
		snap = mucOccSnapshot{room: rec.room, photo: rec.photo, hash: rec.hash, realJID: rec.realJID, prop: rec.prop}
		rec.prop = nil
		if rec.realJID != "" {
			rec.contactTried = true
		}
		return k, snap, true
	}
	return "", mucOccSnapshot{}, false
}

func (s *mucAvatarState) markDone(key, adv string) {
	s.mu.Lock()
	if rec := s.occ[key]; rec != nil {
		rec.doneHash = adv
	}
	s.mu.Unlock()
}

// markWork stamps the start of real work (fetch/apply/remove) for the per-occupant minimum interval.
func (s *mucAvatarState) markWork(key string) {
	s.mu.Lock()
	if rec := s.occ[key]; rec != nil {
		rec.lastWork = s.nowT()
	}
	s.mu.Unlock()
}

func (s *mucAvatarState) markFail(key, adv string) {
	s.mu.Lock()
	if rec := s.occ[key]; rec != nil {
		rec.failHash = adv
		rec.failAt = s.nowT()
	}
	s.mu.Unlock()
}

func (s *mucAvatarState) clearHasGhost(key string) {
	s.mu.Lock()
	if rec := s.occ[key]; rec != nil {
		rec.hasGhost = false
	}
	s.mu.Unlock()
}

func (s *mucAvatarState) lookupMXC(adv string) (mucMXC, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.mxcByHash[adv]
	return e, ok
}

func (s *mucAvatarState) putMXC(e mucMXC, keys ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	if len(s.mxcByHash)+len(keys) > mucAvatarMXCCap {
		s.mxcByHash = make(map[string]mucMXC)
	}
	for _, k := range keys {
		s.mxcByHash[k] = e
	}
}

func (s *mucAvatarState) bump(f func(*mucAvatarStats)) {
	s.mu.Lock()
	f(&s.stats)
	s.statsVer++
	s.mu.Unlock()
}

// noteFailure counts a failed fetch under its class (bounded number of classes).
func (s *mucAvatarState) noteFailure(class string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	s.stats.failed++
	s.statsVer++
	if _, ok := s.failBy[class]; !ok && len(s.failBy) >= mucAvatarFailClasses {
		class = "other"
	}
	s.failBy[class]++
}

// avatarFailClass is the stats key of a fetch error: "<type>/<condition>" for a stanza error,
// "timeout" for an unanswered IQ, "other" otherwise. Never holds JIDs or text from the peer
// beyond the RFC 6120 condition name.
func avatarFailClass(err error) string {
	if typ, cond := avatarErrInfo(err); typ != "" || cond != "" {
		return typ + "/" + cond
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "other"
}

// mucRoomRefusal reports whether a failed occupant vCard fetch says the ROOM refuses to forward
// IQs to occupants (counts toward the per-room breaker), as opposed to an answer about the
// occupant (its server has no vCard, does not implement it, is unreachable, the occupant left),
// which only sets the occupant's failure window. In a big public room the latter are common and
// must not disable fetching for everyone else.
//
// A room refusal is an auth/cancel stanza error that either is attributed to the room (reply from
// the room's bare JID, or error@by naming the room or its service) with a condition that is not
// occupant-specific, or carries a room policy condition (not-allowed, not-acceptable) that no
// other entity claims (error@by naming a different domain).
func mucRoomRefusal(err error, room string) bool {
	var se stanza.Error
	if !errors.As(err, &se) {
		return false // timeouts and transport errors say nothing about the room's policy
	}
	if se.Type != stanza.Cancel && se.Type != stanza.Auth {
		return false
	}
	switch se.Condition {
	case stanza.ItemNotFound, stanza.RecipientUnavailable, stanza.RemoteServerNotFound, stanza.RemoteServerTimeout:
		return false // about the occupant or the route to its server
	}
	roomDomain := room
	if i := strings.IndexByte(room, '@'); i >= 0 {
		roomDomain = room[i+1:]
	}
	by := se.By.String()
	byRoom := by != "" && (strings.EqualFold(bareOf(by), room) || strings.EqualFold(by, roomDomain))
	if byRoom || strings.EqualFold(iqErrorFrom(err), room) {
		return true
	}
	if by != "" {
		return false // another entity (the occupant's server) generated it
	}
	return se.Condition == stanza.NotAllowed || se.Condition == stanza.NotAcceptable
}

// mucAvatarAction is the worker's decision for one job.
type mucAvatarAction int

const (
	mucActDone      mucAvatarAction = iota // nothing to do (mark handled)
	mucActRemove                           // remove the ghost avatar, no IQ
	mucActApplyMXC                         // apply a pre-uploaded avatar, no IQ
	mucActFetch                            // fetch the vCard photo
	mucActSkipUnset                        // photo unknown: nothing, never remove
)

// decideMUCAvatar is the pure decision: ghostHash is the SHA-1 stored for the ghost's current
// avatar, ghostApplied whether that avatar is actually set on Matrix, cached the pre-uploaded
// avatar for the advertised hash (nil if none).
func decideMUCAvatar(snap mucOccSnapshot, ghostHash string, ghostApplied bool, cached *mucMXC) mucAvatarAction {
	switch snap.photo {
	case photoNone:
		if ghostHash != "" {
			return mucActRemove
		}
		return mucActDone
	case photoHash:
		if ghostApplied && snap.hash == ghostHash {
			return mucActDone
		}
		if cached != nil {
			return mucActApplyMXC
		}
		return mucActFetch
	}
	return mucActSkipUnset
}

// runOnce processes at most one job. It reports whether a job was dequeued.
func (s *mucAvatarState) runOnce(ctx context.Context) bool {
	key, snap, ok := s.dequeue()
	if !ok {
		return false
	}
	s.process(ctx, key, snap)
	return true
}

// runWorker is the single per-session worker; it exits when ctx (the session context) ends.
func (s *mucAvatarState) runWorker(ctx context.Context) {
	s.mu.Lock()
	s.initLocked()
	wake := s.wake
	s.mu.Unlock()
	for ctx.Err() == nil {
		if s.runOnce(ctx) {
			s.flushStats(ctx)
			continue
		}
		s.flushStats(ctx)
		t := time.NewTimer(mucAvatarStatsPeriod)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-wake:
			t.Stop()
		case <-t.C:
		}
	}
}

// flushStats logs the cumulative counters at info, at most once per mucAvatarStatsPeriod and only
// if they changed. Counts and room JIDs only (never bytes, URLs or real JIDs).
func (s *mucAvatarState) flushStats(ctx context.Context) {
	s.mu.Lock()
	now := s.nowT()
	if s.statsVer == s.lastStatsVer || (!s.lastStatsAt.IsZero() && now.Sub(s.lastStatsAt) < mucAvatarStatsPeriod) {
		s.mu.Unlock()
		return
	}
	st := s.stats
	s.lastStatsVer = s.statsVer
	s.lastStatsAt = now
	failBy := zerolog.Dict()
	for k, v := range s.failBy {
		failBy = failBy.Int(k, v)
	}
	disabled := make([]string, 0, len(s.roomDisabled))
	for r, d := range s.roomDisabled {
		if d {
			disabled = append(disabled, r)
		}
	}
	occupants, history, queued := len(s.occ), s.histN, len(s.pending)
	s.mu.Unlock()
	s.log(ctx).Info().
		Int("enqueued", st.enqueued).Int("coalesced", st.coalesced).Int("dropped_full", st.droppedFull).
		Int("fetched", st.fetched).Int("applied", st.applied).Int("removed", st.removed).
		Int("failed", st.failed).Int("rejected", st.rejected).Int("skipped_room_disabled", st.skippedDisabled).
		Dict("failures", failBy).Strs("rooms_disabled", disabled).
		Int("occupants", occupants).Int("history_only", history).Int("queued", queued).
		Msg("MUC avatar stats")
}

func (s *mucAvatarState) doPace(ctx context.Context) error {
	if s.pace != nil {
		return s.pace(ctx)
	}
	return nil
}

func (s *mucAvatarState) doSleep(ctx context.Context, d time.Duration) error {
	if s.sleep != nil {
		return s.sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// avatarErrInfo extracts the stanza error type and condition from a fetch error.
func avatarErrInfo(err error) (typ, cond string) {
	var se stanza.Error
	if errors.As(err, &se) {
		return string(se.Type), string(se.Condition)
	}
	return "", ""
}

// process handles one job on the worker goroutine (never on Serve).
func (s *mucAvatarState) process(ctx context.Context, key string, snap mucOccSnapshot) {
	if snap.prop != nil {
		s.processProp(ctx, key, snap)
		return
	}
	if snap.photo == photoUnknown && snap.realJID == "" {
		return
	}
	adv := snap.adv()
	log := s.log(ctx)
	ghost, err := s.getGhost(ctx, key)
	if err != nil {
		log.Debug().Err(err).Str("occupant", key).Msg("MUC avatar: ghost lookup failed")
		return
	}
	if ghost == nil {
		s.clearHasGhost(key)
		log.Debug().Str("occupant", key).Msg("MUC avatar: no ghost for occupant, skipped")
		return
	}
	if snap.realJID != "" && s.applyContact(ctx, key, snap, ghost) {
		return
	}
	if snap.photo == photoUnknown {
		return
	}
	ghostHash := ghost.StoredAvatarHash()
	var cached *mucMXC
	if snap.photo == photoHash {
		if e, ok := s.lookupMXC(snap.hash); ok {
			cached = &e
		}
	}
	act := decideMUCAvatar(snap, ghostHash, ghost.AvatarApplied(), cached)
	switch act {
	case mucActSkipUnset:
		return
	case mucActDone:
		s.markDone(key, adv)
		// Unchanged since an earlier session or job: no IQ and no span, but say so, otherwise a
		// processed job is indistinguishable from a stuck worker in the logs.
		log.Debug().Str("occupant", key).Str("photo", snap.photo.String()).Msg("MUC avatar unchanged, nothing to do")
		return
	}
	s.markWork(key)
	if act == mucActFetch {
		s.mu.Lock()
		disabled := s.roomDisabled[snap.room]
		if disabled {
			s.stats.skippedDisabled++
			s.statsVer++
		}
		s.mu.Unlock()
		if disabled {
			return
		}
	}

	// A job that does real work gets its own root span; ctx is the session context (no span).
	ctx, span := telemetry.Start(ctx, "xmpp.muc_avatar.refresh")
	defer span.End()
	log = s.log(ctx)
	setOutcome := func(source, outcome string) {
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.avatar.source", source), attribute.String("xmpp.avatar.outcome", outcome))
		}
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", snap.room), attribute.String("xmpp.peer.jid", key))
	}

	switch act {
	case mucActRemove:
		applyGhostAvatar(ctx, ghost, &bridgev2.Avatar{Remove: true}, "", nil)
		s.bump(func(st *mucAvatarStats) { st.removed++ })
		setOutcome("occupant", "removed")
		log.Debug().Str("occupant", key).Msg("MUC occupant has no avatar, removed ghost avatar")
		if ghost.AvatarApplied() {
			s.markDone(key, adv)
		}
	case mucActApplyMXC:
		avatar := &bridgev2.Avatar{ID: avatarIDFromHash(cached.realHash), MXC: id.ContentURIString(cached.mxc), Hash: cached.sha256}
		applyGhostAvatar(ctx, ghost, avatar, cached.realHash, nil)
		s.bump(func(st *mucAvatarStats) { st.applied++ })
		setOutcome("cache", "changed")
		log.Debug().Str("occupant", key).Str("photo_hash", cached.realHash).Msg("Applied cached MUC avatar")
		if ghost.AvatarApplied() {
			s.markDone(key, adv)
		}
	case mucActFetch:
		s.fetchAndApply(ctx, key, snap, adv, ghost, ghostHash, setOutcome)
	}
}

// applyContact applies the contact rule (see the file comment) for an occupant whose room reports
// a real JID. It returns false when the contact gives no answer (no ghost, avatar unknown, lookup
// error) and the room-hash path should run. No IQ, no upload, no fetch.
func (s *mucAvatarState) applyContact(ctx context.Context, key string, snap mucOccSnapshot, ghost avatarGhost) bool {
	if s.getContact == nil {
		return false
	}
	log := s.log(ctx)
	c, err := s.getContact(ctx, snap.realJID)
	if err != nil {
		log.Debug().Err(err).Str("occupant", key).Msg("MUC avatar: contact ghost lookup failed")
		return false
	}
	if c.state == contactUnknown {
		return false
	}
	adv := snap.adv()
	settled := s.maybeRefreshContact(ctx, key, adv, c)
	ghostHash := ghost.StoredAvatarHash()
	var avatar *bridgev2.Avatar
	newHash, outcome := "", ""
	switch c.state {
	case contactHas:
		if ghostHash != c.mxc.realHash || !ghost.AvatarApplied() {
			avatar = &bridgev2.Avatar{ID: avatarIDFromHash(c.mxc.realHash), MXC: id.ContentURIString(c.mxc.mxc), Hash: c.mxc.sha256}
			newHash, outcome = c.mxc.realHash, "changed"
		}
	case contactNone:
		if ghostHash != "" {
			avatar = &bridgev2.Avatar{Remove: true}
			outcome = "removed"
		}
	}
	if avatar == nil {
		if settled {
			s.markDone(key, adv)
		} else {
			s.markWork(key) // a later presence after the minimum interval re-checks
		}
		log.Debug().Str("occupant", key).Str("photo", snap.photo.String()).Bool("settled", settled).
			Msg("MUC avatar matches the contact avatar, nothing to do")
		return true
	}
	s.markWork(key)
	ctx, span := telemetry.Start(ctx, "xmpp.muc_avatar.refresh")
	defer span.End()
	log = s.log(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", snap.room), attribute.String("xmpp.peer.jid", key),
			attribute.String("xmpp.avatar.source", "contact_ghost"), attribute.String("xmpp.avatar.outcome", outcome))
	}
	applyGhostAvatar(ctx, ghost, avatar, newHash, nil)
	if avatar.Remove {
		s.bump(func(st *mucAvatarStats) { st.removed++ })
		log.Debug().Str("occupant", key).Msg("MUC avatar removed (contact has none)")
	} else {
		s.bump(func(st *mucAvatarStats) { st.applied++ })
		log.Debug().Str("occupant", key).Str("photo_hash", newHash).Msg("Applied contact avatar to MUC occupant")
	}
	if settled && ghost.AvatarApplied() {
		s.markDone(key, adv)
	}
	return true
}

// maybeRefreshContact handles a room advertising a state that differs from the contact's: the
// contact may be stale (no 1:1 presence told us). It returns settled=false only when a refresh is
// still due but the contact was fetched or refreshed too recently; the caller then leaves the
// advertised state unhandled so a later presence (after the minimum interval) checks again.
// Bounded: one refresh per contact per mucContactRefreshAge and one per advertised state per
// mucAvatarFailRetry; the refresh result only flows back through propagateContact (no loop).
func (s *mucAvatarState) maybeRefreshContact(ctx context.Context, key, adv string, c contactAvatar) (settled bool) {
	if adv == "" || adv == c.adv() || s.refreshContact == nil {
		return true
	}
	now := s.nowT()
	s.mu.Lock()
	s.initLocked()
	last, had := s.contactReq[c.id]
	if had && last.adv == adv && now.Sub(last.at) < mucAvatarFailRetry {
		s.mu.Unlock()
		return true // already refreshed after seeing this advertisement: the room is the stale side
	}
	if now.Sub(c.fetchedAt) < mucContactRefreshAge || (had && now.Sub(last.at) < mucContactRefreshAge) {
		s.mu.Unlock()
		return false
	}
	if len(s.contactReq) >= mucContactReqCap {
		s.contactReq = make(map[string]mucContactReq)
	}
	s.contactReq[c.id] = mucContactReq{adv: adv, at: now}
	s.mu.Unlock()
	s.log(ctx).Debug().Str("occupant", key).Str("advertised", adv).
		Msg("MUC advertised avatar differs from the contact avatar, refreshing contact")
	s.refreshContact(c.id)
	return true
}

// propagateContact is called (from any goroutine, memory only, no IQ) after the 1:1 path applied a
// new avatar (mxc != nil, SHA-1 realHash) or a removal (mxc == nil) for the real bare JID. Every
// occupant of this session whose room-reported real JID matches and which has a ghost gets a
// propagation job that applies the cached avatar. The real JID comes from the room's item jid (the
// room sets it; occupants cannot claim one). The 60 s minimum interval does not apply: no IQ is
// made. The occupant's advertised state is marked handled so a later stale room presence (the
// client did not resend room presence) enqueues nothing and cannot revert the ghost; a different
// advertised hash later still goes through the normal path.
func (s *mucAvatarState) propagateContact(realBare string, mxc *mucMXC) (matched int) {
	if realBare == "" {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initLocked()
	if mxc != nil {
		if len(s.mxcByHash)+1 > mucAvatarMXCCap {
			s.mxcByHash = make(map[string]mucMXC)
		}
		s.mxcByHash[mxc.realHash] = *mxc
	}
	for key, rec := range s.occ {
		if !rec.hasGhost || rec.realJID == "" || !strings.EqualFold(rec.realJID, realBare) {
			continue
		}
		p := &mucProp{remove: mxc == nil}
		if mxc != nil {
			p.mxc = *mxc
		}
		rec.prop = p
		s.enqueueLocked(key)
		if _, ok := s.pending[key]; !ok {
			rec.prop = nil // queue full: do not leave a stale propagation behind
			continue
		}
		rec.doneHash = rec.adv()
		matched++
	}
	return matched
}

// processProp applies a propagation job: no IQ, no fetch, no upload. A room presence with a new
// advertised state that coalesced into this job is requeued for the normal path afterwards.
func (s *mucAvatarState) processProp(ctx context.Context, key string, snap mucOccSnapshot) {
	defer s.requeueIfChanged(key)
	log := s.log(ctx)
	ghost, err := s.getGhost(ctx, key)
	if err != nil {
		log.Debug().Err(err).Str("occupant", key).Msg("MUC avatar: ghost lookup failed")
		return
	}
	if ghost == nil {
		s.clearHasGhost(key)
		return
	}
	p := snap.prop
	ghostHash := ghost.StoredAvatarHash()
	if p.remove {
		if ghostHash == "" {
			return
		}
	} else if ghostHash == p.mxc.realHash && ghost.AvatarApplied() {
		return
	}
	ctx, span := telemetry.Start(ctx, "xmpp.muc_avatar.refresh")
	defer span.End()
	log = s.log(ctx)
	outcome := "changed"
	if p.remove {
		outcome = "removed"
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", snap.room), attribute.String("xmpp.peer.jid", key),
			attribute.String("xmpp.avatar.source", "contact"), attribute.String("xmpp.avatar.outcome", outcome))
	}
	if p.remove {
		applyGhostAvatar(ctx, ghost, &bridgev2.Avatar{Remove: true}, "", nil)
		s.bump(func(st *mucAvatarStats) { st.removed++ })
		log.Debug().Str("occupant", key).Msg("MUC avatar removed (propagated from contact)")
		return
	}
	avatar := &bridgev2.Avatar{ID: avatarIDFromHash(p.mxc.realHash), MXC: id.ContentURIString(p.mxc.mxc), Hash: p.mxc.sha256}
	applyGhostAvatar(ctx, ghost, avatar, p.mxc.realHash, nil)
	s.bump(func(st *mucAvatarStats) { st.applied++ })
	log.Debug().Str("occupant", key).Str("photo_hash", p.mxc.realHash).Msg("Applied cached MUC avatar (propagated from contact)")
}

// requeueIfChanged enqueues key when its advertised state differs from the handled one.
func (s *mucAvatarState) requeueIfChanged(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rec := s.occ[key]; rec != nil && rec.prop == nil && s.shouldEnqueueLocked(rec) {
		s.enqueueLocked(key)
	}
}

func (s *mucAvatarState) fetchAndApply(ctx context.Context, key string, snap mucOccSnapshot, adv string, ghost avatarGhost, ghostHash string, setOutcome func(source, outcome string)) {
	log := s.log(ctx)
	// Honour a pause requested by an earlier resource-constraint style error.
	s.mu.Lock()
	until := s.pausedUntil
	s.mu.Unlock()
	if d := until.Sub(s.nowT()); d > 0 {
		if s.doSleep(ctx, d) != nil {
			return
		}
	}
	occJID, perr := jid.Parse(key)
	if perr != nil {
		s.markDone(key, adv)
		return
	}
	if s.doPace(ctx) != nil {
		return
	}
	source := "occupant"
	data, hash, err := s.fetch(ctx, occJID, key)
	primaryErr := err // the room breaker only ever looks at the answer to the occupant JID
	var primaryType, primaryCond string
	if err != nil && !errors.Is(err, errAvatarRejected) && snap.realJID != "" {
		if realJID, rerr := jid.Parse(snap.realJID); rerr == nil {
			primaryType, primaryCond = avatarErrInfo(err)
			if s.doPace(ctx) != nil {
				return
			}
			source = "real_jid"
			var err2 error
			data, hash, err2 = s.fetch(ctx, realJID, key) // span peer stays the occupant JID
			log.Debug().Str("occupant", key).Str("real_jid", snap.realJID).Bool("ok", err2 == nil).
				Msg("MUC avatar: occupant fetch failed, tried real JID")
			err = err2
		}
	}
	if err != nil && ctx.Err() != nil {
		return // session ended mid-fetch: not the occupant's fault, record nothing
	}
	s.noteRoomAnswer(ctx, snap.room, primaryErr)
	if err != nil {
		if errors.Is(err, errAvatarRejected) {
			s.bump(func(st *mucAvatarStats) { st.rejected++ })
			s.markDone(key, adv)
			setOutcome(source, "rejected")
			log.Debug().Err(err).Str("occupant", key).Str("source", source).Msg("MUC avatar rejected")
			return
		}
		typ, cond := avatarErrInfo(err)
		s.markFail(key, adv)
		s.noteFailure(avatarFailClass(err))
		telemetry.Fail(trace.SpanFromContext(ctx), err)
		setOutcome(source, "error")
		ev := log.Debug().Err(err).Str("occupant", key).Str("source", source).
			Str("error_type", typ).Str("condition", cond).Str("photo_hash", adv)
		if primaryType != "" || primaryCond != "" {
			ev = ev.Str("primary_error_type", primaryType).Str("primary_condition", primaryCond)
		}
		ev.Msg("MUC avatar fetch failed")
		if typ == "wait" {
			s.mu.Lock()
			s.pausedUntil = s.nowT().Add(mucAvatarWaitPause)
			s.mu.Unlock()
		}
		return
	}
	if hash == "" {
		s.bump(func(st *mucAvatarStats) { st.fetched++ })
		s.markDone(key, adv)
		setOutcome(source, "no_photo")
		log.Debug().Str("occupant", key).Str("source", source).Msg("MUC occupant has no avatar")
		return
	}
	s.bump(func(st *mucAvatarStats) { st.fetched++ })
	if hash != snap.hash {
		log.Debug().Str("occupant", key).Str("advertised_hash", snap.hash).Str("photo_hash", hash).
			Msg("MUC avatar hash differs from the advertised hash")
	}
	if hash == ghostHash && ghost.AvatarApplied() {
		s.markDone(key, adv)
		setOutcome(source, "unchanged")
		return
	}
	avatarData := data
	avatar := &bridgev2.Avatar{
		ID:  avatarIDFromHash(hash),
		Get: func(context.Context) ([]byte, error) { return avatarData, nil },
	}
	dataSHA256 := sha256.Sum256(data)
	applyGhostAvatar(ctx, ghost, avatar, hash, func(g *bridgev2.Ghost) {
		// Cache only an MXC that provably holds these bytes (a failed upload leaves the previous
		// AvatarMXC/AvatarHash in place), and only under the hash we computed ourselves: keying by
		// the advertised hash would let an occupant advertise someone else's hash while serving its
		// own photo, poisoning the cache for that other occupant.
		if g.AvatarMXC != "" && g.AvatarHash == dataSHA256 {
			s.putMXC(mucMXC{realHash: hash, mxc: string(g.AvatarMXC), sha256: g.AvatarHash}, hash)
		}
	})
	s.bump(func(st *mucAvatarStats) { st.applied++ })
	setOutcome(source, "changed")
	log.Debug().Str("occupant", key).Str("photo_hash", hash).Int("bytes", len(data)).Str("source", source).
		Msg("Fetched MUC avatar (vCard)")
	if ghost.AvatarApplied() {
		s.markDone(key, adv)
	}
}

// noteRoomAnswer feeds the per-room breaker with the answer to the occupant-JID query (never the
// real-JID fallback, which is the member's own server). A room refusal (mucRoomRefusal) counts;
// an answered query (photo, no photo, or a photo we rejected) proves the room forwards and resets
// the count; any other error (about the occupant, or a timeout) leaves it alone. After
// mucAvatarRoomErrMax consecutive refusals fetching is disabled for the room until the next session.
func (s *mucAvatarState) noteRoomAnswer(ctx context.Context, room string, primaryErr error) {
	if primaryErr == nil || errors.Is(primaryErr, errAvatarRejected) {
		s.mu.Lock()
		if s.roomCancelErrs != nil {
			delete(s.roomCancelErrs, room)
		}
		s.mu.Unlock()
		return
	}
	if !mucRoomRefusal(primaryErr, room) {
		return
	}
	s.mu.Lock()
	s.initLocked()
	s.roomCancelErrs[room]++
	n := s.roomCancelErrs[room]
	disable := n >= mucAvatarRoomErrMax && !s.roomDisabled[room]
	if disable {
		s.roomDisabled[room] = true
		s.statsVer++
	}
	s.mu.Unlock()
	if disable {
		typ, cond := avatarErrInfo(primaryErr)
		s.log(ctx).Info().Str("room", room).Int("refusals", n).Str("error_type", typ).Str("condition", cond).
			Msg("MUC avatar fetching disabled for room (room refuses occupant vCard queries)")
	}
}

// --- xmppClient glue ---

// startMUCAvatars resets the per-session state, installs the production seams and starts the
// worker on the session context. Called next to the session context creation in Connect.
func (x *xmppClient) startMUCAvatars(sessCtx context.Context) {
	s := &x.mucAv
	s.reset()
	s.mu.Lock()
	if s.fetch == nil {
		s.fetch = func(ctx context.Context, to jid.JID, spanPeer string) ([]byte, string, error) {
			data, _, sum, err := x.fetchVCardPhoto(ctx, to, spanPeer)
			return data, sum, err
		}
	}
	if s.getGhost == nil {
		s.getGhost = func(ctx context.Context, occupant string) (avatarGhost, error) {
			if x.conn == nil || x.conn.br == nil {
				return nil, nil
			}
			g, err := x.conn.br.GetExistingGhostByID(ctx, networkid.UserID(occupant))
			if err != nil || g == nil {
				return nil, err
			}
			return realAvatarGhost{g}, nil
		}
	}
	// The pacing ticker belongs to this session, so it is always replaced; the pace closure reads
	// it under s.mu (a previous session's worker may still be finishing a job).
	ticker := time.NewTicker(mucAvatarPace)
	go func() { <-sessCtx.Done(); ticker.Stop() }()
	s.paceC = ticker.C
	if s.pace == nil {
		s.pace = func(ctx context.Context) error {
			s.mu.Lock()
			c := s.paceC
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c:
				return nil
			}
		}
	}
	if s.logger == nil {
		s.logger = x.logFor
	}
	if s.getContact == nil {
		s.getContact = x.loadContactAvatar
	}
	if s.refreshContact == nil {
		s.refreshContact = x.queueAvatarRefresh
	}
	s.mu.Unlock()
	// The worker ctx carries the client logger, so framework errors inside Ghost.UpdateInfo (avatar
	// upload, profile set; logged via zerolog.Ctx) are not silently dropped.
	go s.runWorker(x.log.WithContext(sessCtx))
}

// loadContactAvatar reads the 1:1 contact ghost row for a bare real JID (worker only). It reads a
// fresh row from the DB instead of the shared cached *bridgev2.Ghost, so it never races with an
// in-flight UpdateInfo and never creates a ghost; the row can lag an in-flight 1:1 update, which
// then propagates through propagateContact. Tries the JID as reported, then lowercased.
func (x *xmppClient) loadContactAvatar(ctx context.Context, realBare string) (contactAvatar, error) {
	if x.conn == nil || x.conn.br == nil || x.conn.br.DB == nil {
		return contactAvatar{}, nil
	}
	if networkid.UserLoginID(realBare) == x.loginID {
		return contactAvatar{}, nil // our own account in the room from another client
	}
	ids := []string{realBare}
	if l := strings.ToLower(realBare); l != realBare {
		ids = append(ids, l)
	}
	for _, gid := range ids {
		g, err := x.conn.br.DB.Ghost.GetByID(ctx, networkid.UserID(gid))
		if err != nil {
			return contactAvatar{}, err
		}
		if g != nil {
			meta, _ := g.Metadata.(*GhostMetadata)
			return classifyContactGhost(gid, g.AvatarID, g.AvatarMXC, g.AvatarHash, g.AvatarSet, meta), nil
		}
	}
	return contactAvatar{}, nil
}

// noteMUCPresence is called on the Serve goroutine for MUC occupant presence (memory only).
func (x *xmppClient) noteMUCPresence(occupantFull, room, typ string, info presenceInfo) {
	own := info.hasStatus("110") || x.getMUCOurOccupantJID(room) == occupantFull
	if x.mucAv.notePresence(occupantFull, room, info, typ == "unavailable", own) {
		x.log.Debug().Int("cap", mucOccupantCap).Msg("MUC avatar occupant table full, ignoring new occupants this session")
	}
}

// propagateContactAvatar is called by the 1:1 path after it applied a new avatar (mxc != nil) or
// removal (mxc == nil) for bare JID. Memory only; safe from any goroutine.
func (x *xmppClient) propagateContactAvatar(bareJID string, mxc *mucMXC) {
	x.mucAv.propagateContact(bareJID, mxc)
}

// noteMUCGhost is called from the portal event loop (memory only; the table mutex makes it safe next
// to the Serve and worker goroutines) once ensureMUCGhostDisplayName holds the ghost.
func (x *xmppClient) noteMUCGhost(occupantFull string) {
	if x.mucAv.noteGhost(occupantFull) {
		x.log.Debug().Int("cap", mucOccupantCap).Msg("MUC avatar occupant table full, ignoring new occupants this session")
	}
}

// mucOccupantSeen reports whether a ghost for the occupant JID was seen this session (it spoke), so
// signals from it (read markers) may be bridged without making a ghost join just for them. Memory only.
func (x *xmppClient) mucOccupantSeen(occupantFull string) bool {
	x.mucAv.mu.Lock()
	defer x.mucAv.mu.Unlock()
	rec := x.mucAv.occ[occupantFull]
	return rec != nil && rec.hasGhost
}
