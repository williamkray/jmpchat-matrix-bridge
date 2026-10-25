package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/format"
	"mellium.im/xmlstream"
	"mellium.im/xmpp"
	"mellium.im/xmpp/disco"
	"mellium.im/xmpp/jid"
	"mellium.im/xmpp/roster"
	"mellium.im/xmpp/stanza"
	"mellium.im/xmpp/upload"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

var (
	_ bridgev2.NetworkAPI                             = (*xmppClient)(nil)
	_ bridgev2.IdentifierResolvingNetworkAPI          = (*xmppClient)(nil)
	_ bridgev2.ReactionHandlingNetworkAPI             = (*xmppClient)(nil)
	_ bridgev2.BackfillingNetworkAPI                  = (*xmppClient)(nil)
	_ bridgev2.PersonalFilteringCustomizingNetworkAPI = (*xmppClient)(nil)
)

type xmppClient struct {
	loginID networkid.UserLoginID
	meta    *JabberLoginMeta
	log     zerolog.Logger
	login   *bridgev2.UserLogin
	conn    *JabberConnector
	// queueHook, when set (tests only), receives remote events instead of the bridgev2 queue.
	queueHook func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult
	// noticeHook, when set (tests only), receives portal notices instead of the bridge bot.
	noticeHook func(portal *bridgev2.Portal, text string)
	// sendOutHook, when set (tests only), receives outbound messages built by HandleMatrixEdit instead
	// of the XMPP session.
	sendOutHook func(ctx context.Context, m outMessage) (string, error)
	// sig is the per-session chat signal state (typing peers, dedupe); reset in Connect.
	sig signalState
	// sessionStartedAt (unix nanos, 0 = none) is set by Connect when a session goes live and consumed by
	// ReconnectLoop when that session has ended.
	sessionStartedAt atomic.Int64
	// signalHook, bridgedHook and betweenHook (tests only) replace the signal send, the "row exists"
	// check after a bridged message and the message window query of read receipts.
	signalHook func(ctx context.Context, s outSignal) error
	// doublePuppetHook (tests only) replaces the "user has a double puppet" check of own read receipts.
	doublePuppetHook func() bool
	bridgedHook      func(key networkid.PortalKey, id networkid.MessageID) bool
	betweenHook      func(key networkid.PortalKey, start, end time.Time) ([]*database.Message, error)
	// statusHook, when set (tests only), receives message statuses instead of the Matrix connector.
	statusHook func(ms *bridgev2.MessageStatus, info *bridgev2.MessageStatusEventInfo)
	// rejected dedupes rejection reports per Matrix event (xmpp_error_stanza.go).
	rejected rejectedReports
	// mucNoticeHook / mucNickSaveHook / mucSendJoinHook (tests only) replace the MUC nick notices,
	// the auto-nick persistence and the join presence send.
	mucNoticeHook     func(roomBare, text string)
	mucNickSaveHook   func(roomBare, base, nick string) error
	mucSendJoinHook   func(roomBare, nick string) error
	mucJoinTokensHook func(roomBare string, toks []xml.Token) // the join payload (incl. the bookmark password)
	// mucJoinHistHook, mucLeftHook, mucRejoinPortalsHook and mucNewestMsgHook (tests only) observe the
	// join history request, replace the "left" flag persistence, the portal listing and the newest
	// bridged message lookup of the MUC auto-rejoin (muc_autorejoin.go).
	mucJoinHistHook      func(roomBare string, h *mucHistory)
	mucLeftHook          func(roomBare string, left bool) error
	mucLeaveHook         func(roomBare string) error // replaces the leave presence send
	mucRejoinPortalsHook func(ctx context.Context) ([]*database.Portal, error)
	mucNewestMsgHook     func(ctx context.Context, roomBare string) time.Time
	// mucJoinTracks follows 409 retry chains per room; guarded by mucPendingJoinMu.
	mucJoinTracks map[string]*mucJoinTrack
	// mucJoinGen counts sessions for join state (bumped by resetMUCJoinState); guarded by mucPendingJoinMu.
	mucJoinGen uint64
	// mucJoinWant: rooms this session sent a join for and still wants to be in until confirmed
	// (muc_join_health.go); guarded by mucPendingJoinMu.
	mucJoinWant map[string]*mucJoinWant
	mu          sync.Mutex
	session     *xmpp.Session
	// Connection health (xmpp_conn_health.go). authFailed is set after a final SASL failure and only
	// cleared by a new client (re-login); authFailures/authCond are guarded by mu.
	authFailed      atomic.Bool
	authFailures    int
	lastAuthFailure time.Time // when the last counted not-authorized happened
	authCond        string
	// stateHook / mgmtNoticeHook / closeConnHook (tests only) replace the bridge state send, the
	// management room notice and the keepalive's connection close.
	stateHook      func(status.BridgeState)
	mgmtNoticeHook func(text string)
	closeConnHook  func(sess *xmpp.Session)
	connectHook    func(ctx context.Context) // replaces Connect in ReconnectLoop (tests only)
	// wakeCh wakes a sleeping ReconnectLoop (session ended); created lazily by wakeChan.
	wakeOnce sync.Once
	wakeCh   chan struct{}
	// lastInbound is the unix-nano time of the last stanza read from the server (keepalive idle check).
	lastInbound atomic.Int64
	// pingFunc (tests only) replaces ping.Send in the keepalive.
	pingFunc func(ctx context.Context, sess *xmpp.Session, to jid.JID) error
	// sessionCancel cancels the context shared by this session's background goroutines (roster
	// sync/backfill, MAM catch-up loop). Guarded by mu; see endSessionLocked.
	sessionCancel context.CancelFunc
	uploadJID     *jid.JID // Cached upload service JID (discovered via common subdomains)
	uploadJIDMu   sync.Mutex
	// roster holds the latest fetched XMPP roster for this login, keyed by bare JID.
	// Used to map Jabber contact names to Matrix ghost display names and DM room names.
	roster map[string]string
	// rosterJIDs holds every bare JID of the roster, named or not (roster is names only). rosterLoaded
	// is set by the first complete roster fetch of this client; before that rosterJIDs may be partial.
	// All three are guarded by rosterMu and only written by applyRosterItems.
	rosterJIDs   map[string]struct{}
	rosterLoaded bool
	rosterMu     sync.RWMutex
	// mucCache caches disco-based MUC detection per JID so we don't re-query every time.
	mucCache   map[string]bool
	mucCacheMu sync.RWMutex
	// mucOurOccupantJID maps MUC room JID (bare) to our full occupant JID (room@service/nick) for self-echo filtering.
	mucOurOccupantJID   map[string]string
	mucOurOccupantJIDMu sync.RWMutex
	// avatarFetchHook / avatarStampHook (tests only) replace the vCard fetch and the AvatarFetchedAt stamp.
	avatarFetchHook func(ctx context.Context, bareJID string) ([]byte, string, string, error)
	avatarStampHook func(bareJID string)
	// mucHealAt: last self-heal re-join per room (see healMUCAfterError); guarded by mucHealMu.
	mucHealAt map[string]time.Time
	mucHealMu sync.Mutex
	// mucEcho pairs our sent MUC messages with the room's echo (room stanza-id, occupant-id); see xmpp_muc_echo.go.
	mucEcho mucEchoTable
	// mucPendingJoin maps MUC room JID to the nick we used when sending join presence; cleared when we get our presence or error.
	mucPendingJoin   map[string]string
	mucPendingJoinMu sync.Mutex
	// mam holds state for in-flight MAM (XEP-0313) queries for backfill/catch-up.
	mam mamState
	// lastMAMSync tracks the last MAM catch-up time per portal (key = portal ID) for periodic sync.
	lastMAMSync   map[string]time.Time
	lastMAMSyncMu sync.Mutex
	// knownPortalIDsForMAM holds portal IDs we've seen (from inbound or carbon) so we run MAM catch-up
	// for them even if they're not in the roster (e.g. Cheogram group MMS).
	knownPortalIDsForMAM   map[string]struct{}
	knownPortalIDsForMAMMu sync.Mutex
	// lastBackfillOnView tracks when we last ran mamBackfillOnView per portal (fallback when user sends message).
	lastBackfillOnView   map[string]time.Time
	lastBackfillOnViewMu sync.Mutex
	// carbonsEnableCh receives the result of the carbons enable IQ (nil = success, err = server rejected).
	carbonsEnableCh chan error
	carbonsEnableID string
	carbonsEnableMu sync.Mutex
	// reconnectCancel stops ReconnectLoop; set in LoadUserLogin, called in Disconnect (which LogoutRemote uses).
	reconnectCancel context.CancelFunc
	// callSessions tracks in-flight XEP-0353 Jingle Message Initiation calls, keyed by session id,
	// so accept/reject/finish (which only carry the sid) can be mapped back to the caller's portal.
	callSessions   map[string]*callSession
	callSessionsMu sync.Mutex
	// avatarRefreshing dedups concurrent background vCard avatar fetches per bare JID so a
	// burst of GetUserInfo/presence triggers only fetches once at a time per contact.
	// avatarFetchSem bounds how many of those background fetches run at once (thundering-herd
	// guard for large-roster syncs). Both are lazily initialized in queueAvatarRefresh.
	avatarRefreshing   map[string]bool
	avatarFetchSem     chan struct{}
	avatarRefreshingMu sync.Mutex
	// mucAv is the MUC occupant avatar table, queue and worker state (xep0054_muc.go).
	mucAv mucAvatarState
	// bm is the XEP-0402 bookmark state of this session (xep0402_state.go).
	bm bookmarkState
}

// maxConcurrentAvatarFetches caps simultaneous background vCard avatar fetches.
const maxConcurrentAvatarFetches = 4

const (
	mucFeatureNS      = "http://jabber.org/protocol/muc"
	mucRoomInfoFormNS = "http://jabber.org/protocol/muc#roominfo"
	mucUserNS         = "http://jabber.org/protocol/muc#user"
)

func (x *xmppClient) Connect(ctx context.Context) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.session != nil {
		return
	}
	// The connect span ends when Connect returns; background goroutines run on sessCtx (not derived
	// from this ctx) so they never outlive or inherit the span.
	ctx, span := telemetry.Start(ctx, "xmpp.connect")
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.login.jid", string(x.loginID)))
	}
	if x.authFailed.Load() {
		// The framework calls Connect directly at startup; a rejected login stays rejected.
		x.sendBadCredentials(x.authCond)
		return
	}
	x.sendState(status.BridgeState{StateEvent: status.StateConnecting})
	origin, err := jid.Parse(string(x.loginID))
	if err != nil {
		telemetry.Fail(span, err)
		span.SetAttributes(attribute.String("xmpp.connect.result", "failed"))
		x.sendState(status.BridgeState{StateEvent: status.StateTransientDisconnect, Message: "invalid JID"})
		return
	}
	sess, err := dialXMPPFunc(ctx, origin, x.meta.Password, x.dialOptions(), log)
	if err != nil {
		telemetry.Fail(span, err)
		span.SetAttributes(attribute.String("xmpp.connect.result", "failed"))
		x.handleDialFailure(ctx, err)
		return
	}
	x.authFailures = 0
	x.lastInbound.Store(time.Now().UnixNano())
	x.session = sess
	// The session is live from here, whoever called Connect (the reconnect loop or bridgev2 itself): the
	// loop uses this to back off after a session that dies right after bind.
	x.sessionStartedAt.Store(time.Now().UnixNano())
	// Background goroutines of this session run on sessCtx so they end with the session (Serve loop
	// exit or Disconnect) instead of leaking one set per reconnect.
	sessCtx, cancelSess := context.WithCancel(context.Background())
	x.sessionCancel = cancelSess
	// Join state belongs to the previous session: forget it before anything of this one can join.
	x.resetMUCJoinState()
	x.bm.reset()
	x.sig.reset()
	span.SetAttributes(attribute.String("xmpp.connect.result", "connected"))

	x.sendState(status.BridgeState{StateEvent: status.StateConnected})

	// Send initial presence so the server delivers messages to this resource.
	// Without this, many servers (including Snikket/Prosody) do not push messages to the client.
	// It carries the XEP-0115 caps (<c/>) so our server delivers PEP +notify events (bookmarks) and
	// peers can disco the bridge; MUC join presences deliberately carry none.
	if err := sess.Send(ctx, stanza.Presence{}.Wrap(bridgeCaps().TokenReader())); err != nil {
		log.Warn().Err(err).Msg("Failed to send initial presence; message delivery may not work")
	} else {
		span.AddEvent("initial_presence_sent")
		log.Debug().Msg("Sent initial presence (available)")
	}

	// #region agent log
	log.Debug().Str("hypothesis", "H1").Msg("DEBUG: Connect about to start serveMessages")
	// #endregion
	go x.serveMessages()
	// Release lock so serveMessages can acquire it and run Serve (processes carbons response).
	// Otherwise we deadlock: Connect holds lock, enableCarbons waits for response, but
	// serveMessages blocks on lock so Serve never runs and response is never processed.
	x.mu.Unlock()
	// Brief yield so Serve is reading before we send carbons enable.
	select {
	case <-ctx.Done():
		// #region agent log
		log.Debug().Str("hypothesis", "H5").Msg("DEBUG: Connect ctx.Done during sleep")
		// #endregion
		x.mu.Lock()
		return
	case <-time.After(100 * time.Millisecond):
	}
	// #region agent log
	log.Debug().Str("hypothesis", "H1").Msg("DEBUG: Connect sleep done, calling enableCarbons")
	// #endregion
	// Enable XEP-0280 Message Carbons so we receive copies of messages sent/received on other clients.
	if err := x.enableCarbons(ctx); err != nil {
		log.Warn().Err(err).Msg("Failed to enable message carbons; other-client sync may use MAM catch-up only")
	} else {
		// #region agent log
		log.Debug().Str("hypothesis", "H4").Msg("DEBUG: Connect enableCarbons succeeded")
		// #endregion
		log.Debug().Msg("Enabled message carbons (XEP-0280)")
	}
	// #region agent log
	log.Debug().Str("hypothesis", "H1").Msg("DEBUG: Connect after enableCarbons")
	// #endregion
	x.mu.Lock() // Re-acquire before return (defer will unlock)
	// Fetch the roster on every new session. Besides mapping roster names to ghosts and DM room
	// names, requesting the roster is what makes the server treat this resource as "interested"
	// (RFC 6121 §2.1.6), so it only sends us roster pushes (contact renamed/added/removed on
	// another client) after this. This must run here rather than in ReconnectLoop: the framework
	// also calls Connect directly at startup, and in that case the loop never sees a connect of
	// its own. When backfill.enabled, also run full MAM backfill for roster DMs.
	go x.runFullBackfillAfterRosterSync(sessCtx)
	// Periodic MAM catch-up so messages sent from other clients appear within ~45s if carbons aren't used.
	go x.runMAMCatchUpLoop(sessCtx)
	// MUC occupant avatars: fresh occupant table per session (a rejoin re-sends every presence) and
	// a single worker; presence handling on Serve only touches memory (xep0054_muc.go).
	x.startMUCAvatars(sessCtx)
	// Re-join every bridged MUC of this login (own root span, staggered, ends with sessCtx).
	go x.runMUCAutoRejoin(sessCtx)
	// Live bookmark changes (PEP events queued by the Serve goroutine are applied here).
	go x.runBookmarkEventWorker(sessCtx, x.bm.eventChan())
	// XEP-0199 keepalive: detects a dead connection long before TCP keepalive does.
	go x.runKeepalive(sessCtx, sess, origin.Domain())
	// Join confirmation watchdog and XEP-0410 self-ping of joined rooms (muc_join_health.go).
	go x.runMUCJoinHealth(sessCtx)
}

// endSessionLocked cancels the current session's background goroutine context. Requires x.mu held.
// Callers clear x.session themselves; a later Connect creates a fresh context, so a new session's
// goroutines are never affected by an old session's teardown.
func (x *xmppClient) endSessionLocked() {
	if x.sessionCancel != nil {
		x.sessionCancel()
		x.sessionCancel = nil
	}
}

// enableCarbons sends the XEP-0280 enable IQ and waits for the server response.
// Uses bare JID per XEP-0280. Logs and returns error if server rejects.
func (x *xmppClient) enableCarbons(ctx context.Context) error {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil
	}
	local := sess.LocalAddr()
	if local.String() == "" {
		return nil
	}
	ctx, span := telemetry.StartChild(ctx, "xmpp.carbons.enable", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.iq.namespace", carbonsNS))
	}
	x.carbonsEnableMu.Lock()
	x.carbonsEnableID = "carbons-enable-" + xid.New().String()
	x.carbonsEnableCh = make(chan error, 1)
	x.carbonsEnableMu.Unlock()
	// #region agent log
	log.Debug().Str("hypothesis", "H2").Str("carbonsEnableID", x.carbonsEnableID).Msg("DEBUG: enableCarbons sent IQ, waiting for response")
	// #endregion
	enableStart := xml.StartElement{
		Name: xml.Name{Space: carbonsNS, Local: "enable"},
	}
	payload := xmlstream.Wrap(nil, enableStart)
	iq := stanza.IQ{ID: x.carbonsEnableID, Type: stanza.SetIQ, To: local.Bare()}
	if err := sess.Send(ctx, iq.Wrap(payload)); err != nil {
		x.carbonsEnableMu.Lock()
		x.carbonsEnableID = ""
		x.carbonsEnableCh = nil
		x.carbonsEnableMu.Unlock()
		telemetry.Fail(span, err)
		return err
	}
	select {
	case <-ctx.Done():
		// #region agent log
		log.Debug().Str("hypothesis", "H5").Msg("DEBUG: enableCarbons ctx.Done")
		// #endregion
		telemetry.Fail(span, ctx.Err())
		return ctx.Err()
	case err := <-x.carbonsEnableCh:
		// #region agent log
		log.Debug().Str("hypothesis", "H2").Bool("hasErr", err != nil).Msg("DEBUG: enableCarbons got response from channel")
		// #endregion
		telemetry.Fail(span, err)
		return err
	case <-time.After(5 * time.Second):
		// #region agent log
		log.Debug().Str("hypothesis", "H4").Msg("DEBUG: enableCarbons timeout 5s")
		// #endregion
		x.carbonsEnableMu.Lock()
		x.carbonsEnableID = ""
		x.carbonsEnableCh = nil
		x.carbonsEnableMu.Unlock()
		err := fmt.Errorf("carbons enable: no response from server within 5s")
		telemetry.Fail(span, err)
		return err
	}
}

// tryHandleCarbonsEnableResponse returns true if the IQ was our carbons enable response (consumed and handled).
func (x *xmppClient) tryHandleCarbonsEnableResponse(t xmlstream.TokenReadEncoder, start *xml.StartElement, typeVal, idVal string) bool {
	x.carbonsEnableMu.Lock()
	expectID := x.carbonsEnableID
	ch := x.carbonsEnableCh
	x.carbonsEnableMu.Unlock()
	if expectID == "" || idVal != expectID {
		return false
	}
	drainIQPayload(t)
	var err error
	if typeVal == "error" {
		err = fmt.Errorf("server rejected carbons enable (IQ error)")
		x.log.Warn().Str("iq_id", idVal).Msg("Carbons enable failed: server returned IQ error")
	}
	x.carbonsEnableMu.Lock()
	x.carbonsEnableID = ""
	x.carbonsEnableCh = nil
	x.carbonsEnableMu.Unlock()
	select {
	case ch <- err:
	default:
	}
	return true
}

// drainIQPayload consumes tokens until the closing </iq> (caller has already read the <iq> start).
func drainIQPayload(t xmlstream.TokenReadEncoder) {
	depth := 1
	for depth > 0 {
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

// ReconnectLoop keeps the XMPP session alive with exponential backoff.
// It mirrors the Signal bridge's tryConnect pattern in a simplified form.
func (x *xmppClient) ReconnectLoop(ctx context.Context) {
	backoff := reconnectBackoffStart
	const maxBackoff = 5 * time.Minute

	for {
		select {
		case <-ctx.Done():
			x.log.Debug().Msg("Stopping XMPP reconnect loop (context done)")
			return
		default:
		}
		if x.authFailed.Load() {
			x.log.Warn().Msg("Stopping XMPP reconnect loop: bad credentials")
			return
		}
		if x.IsLoggedIn() {
			// Already connected: wait (a session end wakes the loop) before checking again.
			if !x.reconnectWait(ctx, reconnectPoll) {
				return
			}
			continue
		}

		if started := x.sessionStartedAt.Swap(0); started != 0 {
			// A session (started by this loop or by bridgev2's own Connect call) has ended. One that
			// died right after bind (session limit, conflict, policy violation) must not reconnect at
			// once: keep and double the backoff and wait for it even though the session end woke the loop.
			lived := time.Since(time.Unix(0, started))
			if lived < minHealthySession {
				x.log.Debug().Dur("lived", lived).Dur("backoff", backoff).Msg("XMPP session ended shortly after connecting, backing off")
				t := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					t.Stop()
					return
				case <-t.C:
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			backoff = reconnectBackoffStart
		}

		x.log.Debug().Msg("XMPP reconnect loop: attempting connection")
		// Bound each connect attempt so we don't block forever on a dead server.
		connectCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		if x.connectHook != nil {
			x.connectHook(connectCtx)
		} else {
			x.Connect(connectCtx)
		}
		cancel()

		if x.IsLoggedIn() {
			// Roster sync (and optional full backfill) is started by Connect itself.
			// The backoff is reset only once the session proved healthy (see above). Connect records
			// the start itself; this covers a connect hook that does not.
			x.sessionStartedAt.CompareAndSwap(0, time.Now().UnixNano())
			continue
		}

		if x.authFailed.Load() {
			x.log.Warn().Msg("Stopping XMPP reconnect loop: bad credentials")
			return
		}
		x.log.Debug().Dur("backoff", backoff).Msg("XMPP reconnect loop: connect failed, backing off")
		if !x.reconnectWait(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// reconnectWait sleeps for d, returning early (true) when the loop is woken by a session end and
// false when ctx is done.
func (x *xmppClient) reconnectWait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		x.log.Debug().Msg("Stopping XMPP reconnect loop (context done)")
		return false
	case <-x.wakeChan():
	case <-t.C:
	}
	return true
}

// Disconnect is terminal for this client: the framework only calls it when stopping the bridge,
// replacing the client (re-login with the same account, network reset), or logging out. Stop the
// reconnect loop too, otherwise the old client would reconnect alongside its replacement and
// the account would end up with duplicate XMPP sessions. Unexpected connection drops don't go
// through here (serveMessages just clears the session), so ReconnectLoop still handles those.
func (x *xmppClient) Disconnect() {
	x.mu.Lock()
	if x.reconnectCancel != nil {
		x.reconnectCancel()
		x.reconnectCancel = nil
	}
	hadSession := x.session != nil
	x.sig.reset() // stops pending debounced MUC markers
	if hadSession {
		_ = x.session.Close()
		x.session = nil
	}
	x.endSessionLocked()
	x.mucCacheMu.Lock()
	x.mucCache = nil
	x.mucCacheMu.Unlock()
	x.lastMAMSyncMu.Lock()
	x.lastMAMSync = nil
	x.lastMAMSyncMu.Unlock()
	x.knownPortalIDsForMAMMu.Lock()
	x.knownPortalIDsForMAM = nil
	x.knownPortalIDsForMAMMu.Unlock()
	login := x.login
	x.mu.Unlock()
	if e := x.conn.extension(); e != nil {
		e.loginStopped(x)
	}
	if hadSession && login != nil {
		login.BridgeState.Send(status.BridgeState{StateEvent: status.StateTransientDisconnect})
	}
}

func (x *xmppClient) IsLoggedIn() bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.session != nil
}

func (x *xmppClient) LogoutRemote(ctx context.Context) {
	x.Disconnect()
}

func (x *xmppClient) IsThisUser(ctx context.Context, userID networkid.UserID) bool {
	return networkid.UserLoginID(userID) == x.loginID
}

// SyncRoster fetches the XMPP roster from the server and updates Matrix ghost/room names.
// It is safe to call concurrently. Use this after renaming contacts in another client, since
// many servers do not push roster updates for name-only changes.
func (x *xmppClient) SyncRoster(ctx context.Context) {
	x.syncRoster(ctx)
}

// RunFullBackfill runs MAM backfill for all roster DMs (skips MUC). Implements FullBackfillRunner.
// No-ops if backfill is disabled in config. Used by sync-roster command and optionally after login.
func (x *xmppClient) RunFullBackfill(ctx context.Context) {
	x.runFullBackfill(ctx)
}

// runFullBackfillAfterRosterSync syncs the roster, then runs full MAM backfill when backfill.enabled.
// Called after each successful connect so portals get history before new messages arrive.
func (x *xmppClient) runFullBackfillAfterRosterSync(ctx context.Context) {
	x.syncRoster(ctx)
	if x.conn.backfillConfig == nil || !x.conn.backfillConfig.Enabled {
		return
	}
	x.runFullBackfill(ctx)
}

// SetRosterItem adds or updates a contact in the Jabber roster with the given display name.
// The JID is normalized to lowercase. After the server confirms, we update our cache and
// refresh the ghost and DM portal so Matrix shows the new name.
func (x *xmppClient) SetRosterItem(ctx context.Context, jidStr, displayName string) error {
	jidStr = strings.TrimSpace(strings.ToLower(jidStr))
	if jidStr == "" || !strings.Contains(jidStr, "@") {
		return fmt.Errorf("invalid JID: use user@domain")
	}
	parsed, err := jid.Parse(jidStr)
	if err != nil {
		return fmt.Errorf("invalid JID format: %w", err)
	}
	key := parsed.Bare().String()
	if key == "" {
		return fmt.Errorf("invalid JID")
	}

	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("not connected to Jabber")
	}

	item := roster.Item{JID: parsed, Name: strings.TrimSpace(displayName)}
	setCtx, setSpan := telemetry.StartChild(ctx, "xmpp.roster.set", trace.WithSpanKind(trace.SpanKindClient))
	if setSpan.IsRecording() {
		setSpan.SetAttributes(attribute.String("xmpp.iq.namespace", "jabber:iq:roster"), attribute.String("xmpp.peer.jid", key))
	}
	setErr := roster.Set(setCtx, sess, item)
	telemetry.Fail(setSpan, setErr)
	setSpan.End()
	if setErr != nil {
		return fmt.Errorf("roster set failed: %w", setErr)
	}

	// Update local cache and refresh Matrix so the user sees the new name without waiting for a push.
	x.applyRosterItems(false, rosterEntry{jid: key, name: item.Name})

	x.logFor(ctx).Debug().Str("jid", key).Msg("Roster item set from Matrix") // no name: bulk imports (CardDAV) would log every contact
	x.refreshRosterGhostsAndPortals(ctx, map[string]string{strings.ToLower(key): item.Name})
	return nil
}

// RemoveRosterItem deletes the contact from the roster (RFC 6121 §2.5), drops it from the local cache and
// refreshes the ghost and DM portal so their names fall back to those of an un-rostered contact.
func (x *xmppClient) RemoveRosterItem(ctx context.Context, jidStr string) error {
	jidStr = strings.TrimSpace(strings.ToLower(jidStr))
	if jidStr == "" || !strings.Contains(jidStr, "@") {
		return fmt.Errorf("invalid JID: use user@domain")
	}
	parsed, err := jid.Parse(jidStr)
	if err != nil {
		return fmt.Errorf("invalid JID format: %w", err)
	}
	key := parsed.Bare().String()
	if key == "" {
		return fmt.Errorf("invalid JID")
	}

	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("not connected to Jabber")
	}

	rmCtx, rmSpan := telemetry.StartChild(ctx, "xmpp.roster.remove", trace.WithSpanKind(trace.SpanKindClient))
	if rmSpan.IsRecording() {
		rmSpan.SetAttributes(attribute.String("xmpp.iq.namespace", "jabber:iq:roster"), attribute.String("xmpp.peer.jid", key))
	}
	rmErr := roster.Delete(rmCtx, sess, parsed.Bare())
	telemetry.Fail(rmSpan, rmErr)
	rmSpan.End()
	if rmErr != nil {
		if errors.Is(rmErr, stanza.Error{Condition: stanza.ItemNotFound}) {
			return fmt.Errorf("%s is not in the roster on the server", key)
		}
		return fmt.Errorf("roster remove failed: %w", rmErr)
	}

	// Drop it from the cache like a subscription=remove push; the refresh then reads the un-rostered
	// state (no roster name) for the ghost and the DM room name.
	x.applyRosterItems(false, rosterEntry{jid: key, remove: true})

	x.logFor(ctx).Info().Str("jid", key).Msg("Roster item removed from Matrix")
	x.refreshRosterGhostsAndPortals(ctx, map[string]string{key: ""})
	return nil
}

// syncRoster fetches the XMPP roster for this login and stores it in-memory.
// This allows mapping Jabber contact names to Matrix ghost display names and DM room names.
// It is safe to call concurrently; only one fetch will be in-flight per call.
func (x *xmppClient) syncRoster(ctx context.Context) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return
	}
	ctx, span := telemetry.Start(ctx, "xmpp.roster.sync")
	defer span.End()
	log := x.logFor(ctx)

	newRoster, entries, itemCount, iterErr := x.fetchRoster(ctx, sessionRosterFetcher{sess})
	if iterErr != nil {
		telemetry.Fail(span, iterErr)
		log.Warn().Err(iterErr).Msg("Failed to fetch XMPP roster")
		return
	}
	span.SetAttributes(attribute.Int("roster.count", len(newRoster)))

	// Known race (ROADMAP.md): this replaces the cache with the fetched snapshot, so a roster push or
	// SetRosterItem applied between the server's answer and this point is overwritten until the next sync.
	x.applyRosterItems(true, entries...)

	log.Info().Int("item_count", itemCount).Int("entry_count", len(newRoster)).Msg("Synced XMPP roster for login")

	// Push updated names to Matrix for all roster contacts that have ghosts/portals.
	// Otherwise Matrix keeps showing old display names until the user opens the chat.
	x.refreshRosterGhostsAndPortals(ctx, newRoster)
}

// fetchRoster runs the roster IQ and returns the items (names by lowercased bare JID, the entries and the
// raw item count). The response is closed before it returns: mellium's Serve goroutine hands an IQ result
// to its waiter and then blocks until that response is closed, so every other stanza (including the
// results other IQ waiters are waiting for) queues behind an open roster iterator. It must therefore
// never stay open across slow work such as the ghost/portal refresh (DB + Matrix requests).
func (x *xmppClient) fetchRoster(ctx context.Context, sess rosterFetcher) (names map[string]string, entries []rosterEntry, itemCount int, err error) {
	log := x.logFor(ctx)
	fetchCtx, fetchSpan := telemetry.StartChild(ctx, "xmpp.iq.roster_fetch", trace.WithSpanKind(trace.SpanKindClient))
	defer fetchSpan.End()
	if fetchSpan.IsRecording() {
		fetchSpan.SetAttributes(attribute.String("xmpp.iq.namespace", "jabber:iq:roster"))
	}
	iter := sess.fetchRoster(fetchCtx)
	names = make(map[string]string)
	for iter.Next() {
		item := iter.Item()
		bare := item.JID.Bare().String()
		itemCount++
		log.Info().Str("jid", bare).Str("subscription", item.Subscription).Msg("Roster item from server")
		if bare == "" {
			continue
		}
		// Normalize to lowercase so lookups match bridge (portal/ghost IDs are typically lowercased).
		key := strings.ToLower(bare)
		if item.Name != "" {
			names[key] = item.Name
		}
		entries = append(entries, rosterEntry{jid: key, name: item.Name, remove: item.Subscription == "remove"})
	}
	err = iter.Err()
	if cerr := iter.Close(); cerr != nil {
		log.Debug().Err(cerr).Msg("Failed to close roster iterator")
	}
	telemetry.Fail(fetchSpan, err)
	return names, entries, itemCount, err
}

// rosterIter is the part of *roster.Iter that fetchRoster uses.
type rosterIter interface {
	Next() bool
	Item() roster.Item
	Err() error
	Close() error
}

// rosterFetcher starts a roster fetch (a seam so tests can check that the response is closed early).
type rosterFetcher interface {
	fetchRoster(ctx context.Context) rosterIter
}

// sessionRosterFetcher is the production rosterFetcher over a live session.
type sessionRosterFetcher struct{ sess *xmpp.Session }

func (f sessionRosterFetcher) fetchRoster(ctx context.Context) rosterIter {
	return roster.Fetch(ctx, f.sess)
}

// refreshRosterGhostsAndPortals updates Matrix ghost display names and DM room names
// for each given roster JID so they match the current roster (e.g. after full sync or roster push).
// Keys in rosterMap are already lowercase from syncRoster/handleRosterPush.
func (x *xmppClient) refreshRosterGhostsAndPortals(ctx context.Context, rosterMap map[string]string) {
	if rosterMap == nil || x.login == nil {
		x.log.Debug().Msg("refreshRosterGhostsAndPortals skipped: no roster map or login")
		return
	}
	ctx, span := telemetry.Start(ctx, "bridge.refresh_roster_ghosts")
	defer span.End()
	log := x.logFor(ctx)
	span.SetAttributes(attribute.Int("roster.count", len(rosterMap)))
	for bareJID, rosterName := range rosterMap {
		// Ensure we use the same form the bridge uses (lowercase JID).
		jidForLookup := strings.ToLower(bareJID)
		// Refresh ghost so Matrix shows the roster name.
		ghost, err := x.conn.br.GetExistingGhostByID(ctx, networkid.UserID(jidForLookup))
		if err != nil {
			log.Debug().Err(err).Str("bare_jid", bareJID).Msg("GetExistingGhostByID failed")
			continue
		}
		if ghost == nil {
			log.Debug().Str("bare_jid", jidForLookup).Msg("No existing ghost for roster JID")
		} else {
			info, err := x.GetUserInfo(ctx, ghost)
			if err != nil {
				log.Debug().Err(err).Str("bare_jid", jidForLookup).Msg("GetUserInfo failed")
				continue
			}
			if info != nil && info.Name != nil {
				ghost.UpdateInfo(ctx, info)
				log.Info().Str("bare_jid", jidForLookup).Str("display_name", *info.Name).Msg("Updated ghost from roster")
			}
		}
		// Refresh DM portal room name if it exists.
		portal, err := x.conn.br.GetDMPortal(ctx, x.login.ID, networkid.UserID(jidForLookup))
		if err != nil {
			log.Debug().Err(err).Str("bare_jid", bareJID).Msg("GetDMPortal failed")
			continue
		}
		if portal == nil || portal.MXID == "" {
			log.Debug().Str("bare_jid", jidForLookup).Msg("No DM portal for roster JID")
		} else {
			portal.UpdateBridgeInfo(ctx)
			// Queue ChatResync so the bridge re-fetches GetChatInfo and applies the new room name to Matrix.
			x.queueRemoteEvent(ctx, &simplevent.ChatResync{
				EventMeta: simplevent.EventMeta{
					Type:      bridgev2.RemoteEventChatResync,
					PortalKey: portal.PortalKey,
				},
				GetChatInfoFunc: x.GetChatInfo,
			})
			log.Info().Str("bare_jid", jidForLookup).Str("room_id", portal.MXID.String()).Str("roster_name", rosterName).Msg("Updated portal from roster (queued room name sync)")
		}
	}
	x.refreshGroupMMSPortals(ctx, rosterMap)
}

// refreshGroupMMSPortals queues a ChatResync for this login's Cheogram group MMS portals that
// include any of the given JIDs, since their room names are built from participants' roster names.
func (x *xmppClient) refreshGroupMMSPortals(ctx context.Context, rosterMap map[string]string) {
	log := x.logFor(ctx)
	if !CheogramEnabled(x.conn.br) || len(rosterMap) == 0 {
		return
	}
	portals, err := x.conn.br.DB.Portal.GetAllWithMXID(ctx)
	if err != nil {
		log.Debug().Err(err).Msg("Failed to list portals for group MMS roster refresh")
		return
	}
	for _, p := range portals {
		if p.Receiver != x.loginID {
			continue
		}
		individuals, ok := ParseCheogramGroupMMSJID(string(p.ID))
		if !ok {
			continue
		}
		for _, j := range individuals {
			if _, changed := rosterMap[strings.ToLower(j)]; changed {
				x.queueRemoteEvent(ctx, &simplevent.ChatResync{
					EventMeta: simplevent.EventMeta{
						Type:      bridgev2.RemoteEventChatResync,
						PortalKey: p.PortalKey,
					},
					GetChatInfoFunc: x.GetChatInfo,
				})
				log.Info().Str("portal_id", string(p.ID)).Str("bare_jid", j).Msg("Queued group MMS room name sync from roster")
				break
			}
		}
	}
}

// rosterEntry is one roster item as applied to the cache.
type rosterEntry struct {
	jid    string // lowercase bare JID
	name   string // "" = the contact has no roster name
	remove bool   // subscription=remove: the contact is gone
}

// applyRosterItems is the single write path of the roster cache (sync, push and set). With replace the
// cache is rebuilt from items (a complete fetch) and marked loaded; otherwise items are merged into it.
// A removed item leaves both the name map and the JID set; an unnamed item stays in the JID set only.
func (x *xmppClient) applyRosterItems(replace bool, items ...rosterEntry) {
	x.rosterMu.Lock()
	defer x.rosterMu.Unlock()
	if replace || x.roster == nil {
		x.roster = make(map[string]string)
	}
	if replace || x.rosterJIDs == nil {
		x.rosterJIDs = make(map[string]struct{})
	}
	if replace {
		x.rosterLoaded = true
	}
	for _, it := range items {
		if it.jid == "" {
			continue
		}
		if it.remove {
			delete(x.roster, it.jid)
			delete(x.rosterJIDs, it.jid)
			continue
		}
		x.rosterJIDs[it.jid] = struct{}{}
		if it.name != "" {
			x.roster[it.jid] = it.name
		} else {
			delete(x.roster, it.jid)
		}
	}
}

// inRoster reports whether the bare JID (any case) is a roster entry, named or not. loaded is false
// until a complete roster fetch finished; callers deciding something irreversible must not trust a
// false "in" then.
func (x *xmppClient) inRoster(bareJID string) (in, loaded bool) {
	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()
	_, in = x.rosterJIDs[strings.ToLower(bareJID)]
	return in, x.rosterLoaded
}

// countRoster counts the roster JIDs for which match returns true (e.g. phone contacts), and reports
// whether the roster is loaded. match is called under the roster read lock: it must be cheap and pure.
func (x *xmppClient) countRoster(match func(bareJID string) bool) (n int, loaded bool) {
	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()
	for j := range x.rosterJIDs {
		if match == nil || match(j) {
			n++
		}
	}
	return n, x.rosterLoaded
}

// lookupRosterName returns the roster name for the given bare JID, if available.
// Keys are stored in lowercase so we normalize the lookup.
func (x *xmppClient) lookupRosterName(bareJID string) string {
	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()
	if x.roster == nil {
		return ""
	}
	return x.roster[strings.ToLower(bareJID)]
}

// SearchRoster searches the roster for contacts matching the query string.
// Matches against both JID and display name (case-insensitive partial match).
func (x *xmppClient) SearchRoster(ctx context.Context, query string) ([]RosterContact, error) {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}

	// Ensure roster is synced before searching
	x.ensureRosterSynced(ctx)

	x.rosterMu.RLock()
	defer x.rosterMu.RUnlock()

	if x.roster == nil || len(x.roster) == 0 {
		return []RosterContact{}, nil
	}

	var results []RosterContact
	for jid, name := range x.roster {
		jidLower := strings.ToLower(jid)
		nameLower := strings.ToLower(name)
		// Match if query appears in JID or name
		if strings.Contains(jidLower, query) || strings.Contains(nameLower, query) {
			results = append(results, RosterContact{
				JID:  jid,
				Name: name,
			})
		}
	}

	return results, nil
}

// ensureRosterSynced checks if the roster cache is empty and synchronously syncs it if needed.
// This ensures roster names are available when GetChatInfo builds room names (e.g. from sync-portal).
func (x *xmppClient) ensureRosterSynced(ctx context.Context) {
	x.rosterMu.RLock()
	rosterEmpty := !x.rosterLoaded && len(x.roster) == 0
	x.rosterMu.RUnlock()

	if !rosterEmpty {
		return
	}

	// Check if we're connected before attempting sync
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return
	}

	// Sync roster synchronously so it's available for GetChatInfo
	x.syncRoster(ctx)
}

// handleRosterPush is called by the mellium roster handler when the server sends a roster push IQ
// (contact added, renamed, or removed, e.g. from another client like Cheogram/Conversations). We
// update the in-memory roster cache and refresh the affected ghost and DM/group MMS room names.
//
// Per RFC 6121 §2.1.6 a push carries the complete item as it now stands, so a missing name means
// the contact has no roster name anymore (fall back to the JID). The server only sends pushes to
// resources that have fetched the roster this session; Connect does that on every new session.
func (x *xmppClient) handleRosterPush(ver string, item roster.Item) error {
	bareJID := item.JID.Bare().String()
	if bareJID == "" {
		return nil
	}
	key := strings.ToLower(bareJID)

	x.applyRosterItems(false, rosterEntry{jid: key, name: item.Name, remove: item.Subscription == "remove"})

	// No name in the log line: bulk roster changes (CardDAV import) would log every contact name.
	x.log.Debug().
		Str("bare_jid", key).
		Str("subscription", item.Subscription).
		Msg("Roster push applied")

	// Refresh off the Serve goroutine: this handler runs inside the XMPP read loop, and the ghost/
	// portal updates make Matrix requests (and may lazily fetch an avatar over XMPP, which would
	// deadlock waiting for a response the blocked read loop can never deliver).
	// The goroutine starts its own root span; the handler has no ctx and never hands over a live span.
	subscription := item.Subscription
	go func() {
		ctx, span := telemetry.Start(x.log.WithContext(context.Background()), "xmpp.roster.push", trace.WithSpanKind(trace.SpanKindConsumer))
		defer span.End()
		if span.IsRecording() {
			span.SetAttributes(attribute.String("xmpp.peer.jid", key), attribute.String("xmpp.roster.subscription", subscription))
		}
		x.refreshRosterGhostsAndPortals(ctx, map[string]string{key: item.Name})
	}()
	return nil
}

// isMUCJID is a fallback heuristic when disco is unavailable (offline or disco failed).
func isMUCJID(jidStr string) bool {
	return strings.Contains(jidStr, "@conference.") || strings.Contains(jidStr, "@muc.") ||
		strings.Contains(jidStr, "@groups.") // Snikket
}

// portalIsMUC returns whether the portal should be treated as a MUC (groupchat, MUC join).
// Uses creation path / stored type first: portal.RoomType from the framework; only falls back
// to disco (isMUC) when RoomType is unset (e.g. legacy portals).
func (x *xmppClient) portalIsMUC(ctx context.Context, portal *bridgev2.Portal) bool {
	if portal == nil {
		return false
	}
	switch portal.RoomType {
	case database.RoomTypeGroupDM:
		return true
	case database.RoomTypeDM:
		return false
	}
	// Unset or other: fall back to disco so legacy portals keep working.
	return x.isMUC(ctx, string(portal.ID))
}

// discoInfoTraced runs a disco#info query (10s cap) inside an xmpp.iq.disco_info child span.
func (x *xmppClient) discoInfoTraced(ctx context.Context, sess *xmpp.Session, to jid.JID) (disco.Info, error) {
	ctx, span := telemetry.StartChild(ctx, "xmpp.iq.disco_info", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.iq.namespace", "http://jabber.org/protocol/disco#info"),
			attribute.String("xmpp.peer.jid", to.String()),
		)
	}
	discoCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	info, err := disco.GetInfo(discoCtx, "", to, sess)
	telemetry.Fail(span, err)
	return info, err
}

// isMUC returns whether the JID is a MUC room (XEP-0045), using service discovery (XEP-0030)
// when connected; result is cached per JID. Falls back to isMUCJID when offline or on disco error.
// Cheogram 1:1 (SMS) JIDs are never MUCs: we must send chat/normal and must not send MUC join.
func (x *xmppClient) isMUC(ctx context.Context, jidStr string) bool {
	if IsCheogramDMJID(jidStr) {
		return false
	}
	x.mucCacheMu.RLock()
	cached, ok := x.mucCache[jidStr]
	x.mucCacheMu.RUnlock()
	if ok {
		return cached
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return isMUCJID(jidStr)
	}
	toJID, err := jid.Parse(jidStr)
	if err != nil {
		return isMUCJID(jidStr)
	}
	info, err := x.discoInfoTraced(ctx, sess, toJID)
	if err != nil {
		x.logFor(ctx).Debug().Err(err).Str("jid", jidStr).Msg("Disco info failed; using MUC heuristic")
		return isMUCJID(jidStr)
	}
	isMUC := false
	for _, f := range info.Features {
		if f.Var == mucFeatureNS {
			isMUC = true
			break
		}
	}
	x.mucCacheMu.Lock()
	if x.mucCache == nil {
		x.mucCache = make(map[string]bool)
	}
	x.mucCache[jidStr] = isMUC
	x.mucCacheMu.Unlock()
	return isMUC
}

func (x *xmppClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	ctx, span := telemetry.Start(ctx, "matrix.get_chat_info")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.peer.jid", string(portal.ID)))
		setPortalTypeAttr(span, portal)
	}
	info, err := x.getChatInfo(ctx, portal)
	telemetry.Fail(span, err)
	return info, err
}

// setPortalTypeAttr records bridge.portal_type from the stored room type (no disco lookup).
func setPortalTypeAttr(span trace.Span, portal *bridgev2.Portal) {
	switch portal.RoomType {
	case database.RoomTypeGroupDM:
		span.SetAttributes(attribute.String("bridge.portal_type", "muc"))
	case database.RoomTypeDM:
		span.SetAttributes(attribute.String("bridge.portal_type", "dm"))
	}
}

func (x *xmppClient) getChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	portalID := string(portal.ID)
	if portalID == "" {
		return nil, nil
	}

	// Prefer stored type (creation path): framework sets portal.RoomType from our previous ChatInfo.
	switch portal.RoomType {
	case database.RoomTypeDM:
		// Ensure roster is synced before building DM ChatInfo so roster names are available
		x.ensureRosterSynced(ctx)
		return x.getDecoratedDMChatInfo(ctx, portal, portalID)
	case database.RoomTypeGroupDM:
		return x.getMUCChatInfo(ctx, portal, portalID)
	}

	// No stored type: use heuristics then disco (like other clients: creation path / JID pattern).
	if IsCheogramDMJID(portalID) {
		// Ensure roster is synced before building DM ChatInfo so roster names are available
		x.ensureRosterSynced(ctx)
		return x.getDecoratedDMChatInfo(ctx, portal, portalID)
	}
	// isMUC asks the server (disco#info, cached) and falls back to the isMUCJID heuristic when
	// offline or disco fails. GetChatInfo runs from bridgev2 portal/command goroutines (also the
	// ChatResync GetChatInfoFunc), never on the mellium Serve goroutine, so the IQ round trip is safe.
	if x.isMUC(ctx, portalID) {
		return x.getMUCChatInfo(ctx, portal, portalID)
	}

	// Default to DM - ensure roster is synced before building ChatInfo
	x.ensureRosterSynced(ctx)
	return x.getDecoratedDMChatInfo(ctx, portal, portalID)
}

// getDecoratedDMChatInfo builds DM ChatInfo and lets the connector extension (if any) adjust it.
func (x *xmppClient) getDecoratedDMChatInfo(ctx context.Context, portal *bridgev2.Portal, portalID string) (*bridgev2.ChatInfo, error) {
	info, err := x.getDMChatInfo(ctx, portal, portalID)
	if err == nil {
		x.conn.decorateChatInfo(portalID, info)
	}
	return info, err
}

// CustomizePersonalFilteringSpace implements bridgev2.PersonalFilteringCustomizingNetworkAPI.
// No customization: use standard bridge behavior for the personal filtering space room.
func (x *xmppClient) CustomizePersonalFilteringSpace(req *mautrix.ReqCreateRoom) {}

// plainFromFormattedBody converts a Matrix formatted_body (HTML) to the plain text sent as the XMPP body
// when an event carries no plain body. Never send the raw HTML.
func plainFromFormattedBody(html string) string {
	return strings.TrimSpace(format.HTMLToText(html))
}

// composeMUCTopic is the room topic rule shared by the resync (disco#info) and the live subject
// (XEP-0045 section 8.1): the subject when non-empty, else the room description.
func composeMUCTopic(description, subject string) string {
	if subject != "" {
		return subject
	}
	return description
}

// getMUCRoomMetadata fetches MUC room name and topic from disco#info (XEP-0030) extended info (XEP-0128 muc#roominfo form).
// Returns name and topic; empty strings if unavailable or on error.
func (x *xmppClient) getMUCRoomMetadata(ctx context.Context, roomJID string) (name, topic string) {
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return "", ""
	}
	toJID, err := jid.Parse(roomJID)
	if err != nil {
		return "", ""
	}
	info, err := x.discoInfoTraced(ctx, sess, toJID)
	if err != nil {
		x.logFor(ctx).Debug().Err(err).Str("room", roomJID).Msg("Disco info for MUC metadata failed")
		return "", ""
	}
	// Use first conference identity name as room name if present.
	for _, id := range info.Identity {
		if id.Category == "conference" && id.Name != "" {
			name = id.Name
			break
		}
	}
	// Parse XEP-0128 extended forms for muc#roominfo (description, subject, room name).
	for i := range info.Form {
		f := &info.Form[i]
		formType, _ := f.GetString("FORM_TYPE")
		if formType != mucRoomInfoFormNS {
			continue
		}
		// Prefer room name from form; the topic is composeMUCTopic(description, subject).
		if n, ok := f.GetString("muc#roominfo_roomname"); ok && n != "" {
			name = n
		}
		if n, ok := f.GetString("muc#roomconfig_roomname"); ok && n != "" && name == "" {
			name = n
		}
		desc, _ := f.GetString("muc#roominfo_description")
		subj, _ := f.GetString("muc#roominfo_subject")
		topic = composeMUCTopic(sanitizeSubject(capSubject(desc)), sanitizeSubject(capSubject(subj)))
		// Fallback field names used by some servers
		if name == "" {
			if n, ok := f.GetString("Room name"); ok && n != "" {
				name = n
			}
		}
		if topic == "" {
			if s, ok := f.GetString("Subject"); ok && s != "" {
				topic = sanitizeSubject(capSubject(s))
			}
		}
		if topic == "" {
			if d, ok := f.GetString("Description"); ok && d != "" {
				topic = sanitizeSubject(capSubject(d))
			}
		}
		break
	}
	return name, topic
}

// getMUCChatInfo returns ChatInfo for a MUC portal. Occupant list is not yet available until join (3.2).
// When connected, fetches room name and topic from disco#info (XEP-0128) and uses them for Matrix room metadata.
func (x *xmppClient) getMUCChatInfo(ctx context.Context, portal *bridgev2.Portal, roomJID string) (*bridgev2.ChatInfo, error) {
	// Defaults: local part + JID for name, generic topic.
	localPart, _, _ := strings.Cut(roomJID, "@")
	name := localPart
	if name == "" {
		name = roomJID
	}
	name = fmt.Sprintf("%s (%s)", name, roomJID)
	topic := fmt.Sprintf("Jabber MUC — %s", roomJID)
	// Override with disco room metadata when available.
	if metaName, metaTopic := x.getMUCRoomMetadata(ctx, roomJID); metaName != "" || metaTopic != "" {
		if metaName != "" {
			name = metaName
			// Keep JID in parentheses so room list still shows address.
			if !strings.Contains(name, "("+roomJID+")") {
				name = fmt.Sprintf("%s (%s)", name, roomJID)
			}
		}
		if metaTopic != "" {
			topic = metaTopic
		}
	}
	// Append login JID to room name to identify which bridged account it's associated with
	name = fmt.Sprintf("%s (%s)", name, string(x.loginID))
	rt := database.RoomTypeGroupDM
	meID := networkid.UserID(x.loginID)
	// Minimal members: just the bridge user so they get invited. Full occupant list comes with join (3.2).
	members := &bridgev2.ChatMemberList{
		IsFull:           false,
		TotalMemberCount: 1,
		MemberMap: bridgev2.ChatMemberMap{
			meID: {
				EventSender: x.makeEventSender(string(x.loginID)),
				Membership:  event.MembershipJoin,
			},
		},
	}
	// The placeholder/disco topic is only for portal creation. Once the Matrix room exists the live
	// subject (queueMUCSubject, re-sent by the room on every join) owns the topic; returning one here
	// would flip it back and forth on every rejoin resync.
	topicPtr := &topic
	if portal != nil && portal.MXID != "" {
		topicPtr = nil
	}
	return &bridgev2.ChatInfo{
		Name:    &name,
		Topic:   topicPtr,
		Type:    &rt,
		Members: members,
		ExtraUpdates: func(ctx context.Context, p *bridgev2.Portal) bool {
			meta, _ := p.Metadata.(*PortalMetadata)
			if meta != nil {
				meta.LastSync.Time = time.Now()
				return true
			}
			return false
		},
	}, nil
}

// dmDisplayName is the Matrix room name of the DM (or Cheogram group MMS) portal with jidStr: the roster
// name (all participants' roster names or JIDs, comma separated, for a group MMS) else the JID, followed
// by " (login JID)" to identify the bridged account. Shared by getDMChatInfo and dmCreateChatResponse so
// a new portal is named as it will be after its first resync.
func (x *xmppClient) dmDisplayName(jidStr string) string {
	name := jidStr
	if individuals, ok := ParseCheogramGroupMMSJID(jidStr); ok {
		parts := make([]string, 0, len(individuals))
		for _, j := range individuals {
			if rosterName := x.lookupRosterName(j); rosterName != "" {
				parts = append(parts, rosterName)
			} else {
				parts = append(parts, j)
			}
		}
		if len(parts) > 0 {
			name = strings.Join(parts, ", ")
		}
	} else if rosterName := x.lookupRosterName(jidStr); rosterName != "" {
		name = rosterName
	}
	return fmt.Sprintf("%s (%s)", name, string(x.loginID))
}

// getDMChatInfo returns ChatInfo for a DM portal.
// For Cheogram group MMS JIDs (e.g. +1a,+1b,+1c@cheogram.com), returns one member per number so each gets a ghost with roster display name.
func (x *xmppClient) getDMChatInfo(ctx context.Context, portal *bridgev2.Portal, bareJID string) (*bridgev2.ChatInfo, error) {
	name := x.dmDisplayName(bareJID)
	rt := database.RoomTypeDM
	meID := networkid.UserID(x.loginID)
	memberMap := bridgev2.ChatMemberMap{
		meID: {
			EventSender: x.makeEventSender(string(x.loginID)),
			Membership:  event.MembershipJoin,
		},
	}
	var otherID networkid.UserID
	var totalCount int

	if individuals, ok := ParseCheogramGroupMMSJID(bareJID); ok {
		totalCount = 1 + len(individuals)
		for _, j := range individuals {
			uid := networkid.UserID(j)
			memberMap[uid] = bridgev2.ChatMember{
				EventSender: x.makeEventSender(j),
				Membership:  event.MembershipJoin,
			}
			if otherID == "" {
				otherID = uid
			}
		}
	} else {
		totalCount = 2
		otherID = networkid.UserID(portal.ID)
		memberMap[otherID] = bridgev2.ChatMember{
			EventSender: x.makeEventSender(string(portal.ID)),
			Membership:  event.MembershipJoin,
		}
	}

	members := &bridgev2.ChatMemberList{
		IsFull:           true,
		TotalMemberCount: totalCount,
		OtherUserID:      otherID,
		MemberMap:        memberMap,
	}
	return &bridgev2.ChatInfo{
		Name:    &name,
		Type:    &rt,
		Members: members,
		ExtraUpdates: func(ctx context.Context, p *bridgev2.Portal) bool {
			meta, _ := p.Metadata.(*PortalMetadata)
			if meta != nil {
				meta.LastSync.Time = time.Now()
				return true
			}
			return false
		},
	}, nil
}

// decodeBridgeUserID reverses the bridge's encoding of JIDs in Matrix localpart (=40 → @, =2f → /).
// So we can treat ghost.ID as a JID when it's stored in encoded form.
func decodeBridgeUserID(encoded string) string {
	s := strings.ReplaceAll(encoded, "=2f", "/")
	s = strings.ReplaceAll(s, "=40", "@")
	return s
}

// withAppServiceIDSuffix appends " (appservice.id)" to the display name when appservice ID is configured.
// Ghost display names should show the appservice ID so users can distinguish ghosts from multiple bridges.
func (x *xmppClient) withAppServiceIDSuffix(name string) string {
	if x.conn == nil || x.conn.appServiceID == "" {
		return name
	}
	return name + " (" + x.conn.appServiceID + ")"
}

// ensureMUCGhostDisplayName ensures the ghost for the given MUC occupant (room@conf/nick) has the correct display name (nick).
// Call before queueing a MUC message so Matrix shows the short nick instead of the full JID.
func (x *xmppClient) ensureMUCGhostDisplayName(ctx context.Context, occupantFull string) {
	log := x.logFor(ctx)
	if x.conn == nil || x.conn.br == nil || occupantFull == "" || !strings.Contains(occupantFull, "/") {
		return
	}
	ghost, err := x.conn.br.GetGhostByID(ctx, networkid.UserID(occupantFull))
	if err != nil {
		log.Debug().Err(err).Str("occupant", occupantFull).Msg("Could not get ghost for MUC sender display name")
		return
	}
	if ghost == nil {
		return
	}
	// Memory only: lets the MUC avatar worker consider this occupant (see xep0054_muc.go).
	x.noteMUCGhost(occupantFull)
	info, err := x.GetUserInfo(ctx, ghost)
	if err != nil || info == nil || info.Name == nil {
		return
	}
	ghost.UpdateInfo(ctx, info)
	log.Info().Str("occupant", occupantFull).Str("display_name", *info.Name).Msg("Updated MUC ghost display name")
}

func (x *xmppClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	ctx, span := telemetry.Start(ctx, "matrix.get_user_info")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.peer.jid", decodeBridgeUserID(string(ghost.ID))))
	}
	info, err := x.getUserInfo(ctx, ghost)
	if span.IsRecording() && info != nil {
		span.SetAttributes(attribute.Bool("avatar_cached", info.Avatar != nil))
	}
	telemetry.Fail(span, err)
	return info, err
}

func (x *xmppClient) getUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	jidStr := string(ghost.ID)
	if jidStr == "" {
		return nil, nil
	}
	// Bridge may store JID with =40 for @ and =2f for / in ghost ID; decode so we can parse room/nick.
	jidStr = decodeBridgeUserID(jidStr)
	// Cheogram group MMS combined JID (rare: bridge usually only creates ghosts for individual numbers).
	if individuals, ok := ParseCheogramGroupMMSJID(jidStr); ok {
		displayName := "Group MMS (" + strconv.Itoa(len(individuals)) + ")"
		if len(individuals) <= 3 {
			displayName = strings.Join(individuals, ", ")
		}
		dn := x.withAppServiceIDSuffix(displayName)
		return &bridgev2.UserInfo{
			Identifiers: []string{jidStr},
			Name:        &dn,
		}, nil
	}
	// MUC occupant: ghost ID is room@conference.domain/nick — use only the nick as display name so Matrix shows the short nick, not the full JID.
	displayName := jidStr
	isMUCOccupant := false
	if idx := strings.LastIndex(jidStr, "/"); idx >= 0 && idx+1 < len(jidStr) {
		displayName = jidStr[idx+1:]
		isMUCOccupant = true
		x.logFor(ctx).Info().Str("ghost_id", string(ghost.ID)).Str("jid", jidStr).Str("display_name", displayName).Msg("GetUserInfo: MUC occupant, using nick as display name")
	} else if rosterName := x.lookupRosterName(jidStr); rosterName != "" {
		displayName = rosterName
	}
	displayName = x.withAppServiceIDSuffix(displayName)
	isBot := false
	// Inbound avatar (XEP-0054 vcard-temp) for 1:1 contacts only, not MUC occupants. This never
	// blocks: it returns the cached avatar (if any) and refreshes stale avatars in the background.
	// MUC occupants always get a nil Avatar here on purpose: the MUC avatar worker (xep0054_muc.go)
	// is their sole avatar writer, and a stale Avatar from this path could revert or corrupt it.
	var avatar *bridgev2.Avatar
	if !isMUCOccupant {
		meta, _ := ghost.Metadata.(*GhostMetadata)
		avatar = x.cachedContactAvatar(jidStr, meta)
	}
	// Aligned with Signal bridge: include ExtraUpdates for metadata tracking
	return &bridgev2.UserInfo{
		IsBot:       &isBot,
		Identifiers: []string{jidStr},
		Name:        &displayName,
		Avatar:      avatar,
		ExtraUpdates: func(ctx context.Context, g *bridgev2.Ghost) (changed bool) {
			meta, _ := g.Metadata.(*GhostMetadata)
			if meta != nil {
				// Update profile fetched timestamp
				meta.ProfileFetchedAt.Time = time.Now()
				return true
			}
			return false
		},
	}, nil
}

// ResolveIdentifier resolves a JID string (e.g. user@example.com) so the bridge can start a DM
// via the generic start-chat / resolve-identifier flow. When Cheogram config is enabled, phone
// numbers and short codes (e.g. +14169938000 or 33733) are resolved to Cheogram JIDs.
func (x *xmppClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	ctx, span := telemetry.Start(ctx, "matrix.resolve_identifier")
	defer span.End()
	span.SetAttributes(attribute.Bool("create_chat", createChat))
	resp, err := x.resolveIdentifier(ctx, identifier, createChat)
	if span.IsRecording() && resp != nil {
		span.SetAttributes(attribute.String("xmpp.peer.jid", string(resp.UserID)))
	}
	telemetry.Fail(span, err)
	return resp, err
}

func (x *xmppClient) resolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	log := x.logFor(ctx)
	trimmed := strings.TrimSpace(identifier)
	if trimmed == "" {
		warnIfDebugIdentifiers(&x.log)
		log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: empty identifier")
		return nil, fmt.Errorf("invalid JID: must be like user@domain.example")
	}
	jidStr := trimmed
	if !strings.Contains(trimmed, "@") {
		if CheogramEnabled(x.conn.br) {
			// Special case: allow cheogram.com bot (domain-only JID) when Cheogram is enabled
			if strings.ToLower(trimmed) == cheogramDomain {
				jidStr = cheogramDomain
			} else if strings.Contains(trimmed, ",") {
				if resolved, ok := ResolveCheogramMultiIdentifier(trimmed, true); ok {
					jidStr = strings.ToLower(resolved)
				} else {
					warnIfDebugIdentifiers(&x.log)
					log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: comma-separated list but Cheogram multi resolve failed")
					return nil, fmt.Errorf("invalid JID: use user@domain or comma-separated phone numbers when Cheogram is enabled")
				}
			} else if resolved, ok := ResolveCheogramIdentifier(trimmed, true); ok {
				jidStr = strings.ToLower(resolved)
			} else {
				warnIfDebugIdentifiers(&x.log)
				log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: not a JID and Cheogram resolution failed")
				return nil, fmt.Errorf("invalid JID: use user@domain or enable Cheogram for phone/short codes")
			}
		} else {
			warnIfDebugIdentifiers(&x.log)
			log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: invalid JID (missing @)")
			return nil, fmt.Errorf("invalid JID: must be like user@domain.example")
		}
	} else {
		jidStr = strings.ToLower(trimmed)
	}
	if !strings.Contains(jidStr, ",") {
		parsed, err := jid.Parse(jidStr)
		if err != nil {
			warnIfDebugIdentifiers(&x.log)
			log.Debug().Str("identifier", identifier).Msg("ResolveIdentifier: invalid JID format")
			return nil, fmt.Errorf("invalid JID format (use user@domain)")
		}
		jidStr = parsed.String()
	}

	userID := networkid.UserID(jidStr)
	baseName := jidStr
	// For group MMS, resolve to first individual so the bridge doesn't create a ghost for the combined JID.
	if individuals, ok := ParseCheogramGroupMMSJID(jidStr); ok && len(individuals) > 0 {
		userID = networkid.UserID(individuals[0])
		// Name for display can stay as combined JID or use first roster name; getDMChatInfo will refine.
		baseName = individuals[0]
	}
	if rosterName := x.lookupRosterName(baseName); rosterName != "" {
		baseName = rosterName
	}
	name := x.withAppServiceIDSuffix(baseName)
	resp := &bridgev2.ResolveIdentifierResponse{
		UserID: userID,
		UserInfo: &bridgev2.UserInfo{
			Identifiers: []string{jidStr},
			Name:        &name,
		},
	}
	if createChat {
		resp.Chat = x.dmCreateChatResponse(jidStr)
		// Backfill on PM creation: run before room is created so history appears before first message.
		if x.conn.backfillConfig != nil && x.conn.backfillConfig.Enabled {
			backfillCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			x.mamBackfillOnView(x.log.WithContext(backfillCtx), jidStr)
			cancel()
		}
	}
	return resp, nil
}

// dmCreateChatResponse builds the CreateChatResponse for a DM (or group MMS) portal with jidStr.
// It does no network I/O (in particular no backfill).
func (x *xmppClient) dmCreateChatResponse(jidStr string) *bridgev2.CreateChatResponse {
	rt := database.RoomTypeDM
	meID := networkid.UserID(x.loginID)
	memberMap := bridgev2.ChatMemberMap{
		meID: {
			EventSender: x.makeEventSender(string(x.loginID)),
			Membership:  event.MembershipJoin,
		},
	}
	var otherID networkid.UserID
	var totalCount int
	if individuals, ok := ParseCheogramGroupMMSJID(jidStr); ok {
		totalCount = 1 + len(individuals)
		for _, j := range individuals {
			uid := networkid.UserID(j)
			memberMap[uid] = bridgev2.ChatMember{
				EventSender: x.makeEventSender(j),
				Membership:  event.MembershipJoin,
			}
			if otherID == "" {
				otherID = uid
			}
		}
	} else {
		totalCount = 2
		otherID = networkid.UserID(jidStr)
		memberMap[otherID] = bridgev2.ChatMember{
			EventSender: x.makeEventSender(jidStr),
			Membership:  event.MembershipJoin,
		}
	}
	name := x.dmDisplayName(jidStr)
	resp := &bridgev2.CreateChatResponse{
		PortalKey: networkid.PortalKey{
			ID:       networkid.PortalID(jidStr),
			Receiver: x.loginID,
		},
		PortalInfo: &bridgev2.ChatInfo{
			Name: &name,
			Type: &rt,
			Members: &bridgev2.ChatMemberList{
				IsFull:           true,
				TotalMemberCount: totalCount,
				OtherUserID:      otherID,
				MemberMap:        memberMap,
			},
			ExtraUpdates: func(ctx context.Context, p *bridgev2.Portal) bool {
				meta, _ := p.Metadata.(*PortalMetadata)
				if meta != nil {
					meta.LastSync.Time = time.Now()
					return true
				}
				return false
			},
		},
	}
	x.conn.decorateChatInfo(jidStr, resp.PortalInfo)
	return resp
}

// getPreferredMUCNick returns the nick to use for joining the given MUC: a remembered automatic
// alternative (see chooseMUCNick), else room-specific preferred, default preferred, or the login
// localpart / "bridge".
func (x *xmppClient) getPreferredMUCNick(ctx context.Context, roomJIDStr string) string {
	_, nick := x.mucNickChoice(ctx, roomJIDStr)
	return nick
}

// errMUCLeft is returned by joinMUCForPortal for a room the user left (explicit leave command, or a
// bookmark that another client set to not auto-join): only the join command brings it back.
var errMUCLeft = errors.New("room was left; use the join command to return")

// JoinMUC is the explicit join command (MUCJoiner): it ends an earlier "leave", sends the join presence
// and, once the join is confirmed, creates/enables the room's XEP-0402 bookmark. Nick: see chooseMUCNick.
func (x *xmppClient) JoinMUC(ctx context.Context, roomJIDStr string) error {
	ctx, span := telemetry.Start(ctx, "xmpp.muc.join", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomJIDStr))
	}
	if err := x.setMUCLeft(ctx, roomJIDStr, false); err != nil {
		x.logFor(ctx).Warn().Err(err).Str("room", roomJIDStr).Msg("Failed to clear MUC left flag")
	}
	err := x.joinMUCOpts(ctx, roomJIDStr, nil, true)
	telemetry.Fail(span, err)
	return err
}

// joinMUCForPortal makes sure we are in a bridged room when the user views it, sends into it or
// retracts in it (a no-op when already joined). It is not a new "I want this room" intent: it never clears the left flag (a room the
// user left, here or via a bookmark on another client, stays left: errMUCLeft) and never writes a bookmark.
func (x *xmppClient) joinMUCForPortal(ctx context.Context, roomJIDStr string) error {
	if x.isMUCLeft(ctx, roomJIDStr) {
		return errMUCLeft
	}
	ctx, span := telemetry.Start(ctx, "xmpp.muc.join", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomJIDStr))
	}
	// Already in the room (joined and confirmed): send nothing. Re-sending the join presence makes the
	// room replay the full occupant list and default history, which floods a busy room over a
	// rate-limited s2s link and delays every message behind it. A room that dropped us silently is
	// healed by the not-acceptable handling in healMUCAfterError instead.
	if x.getMUCOurOccupantJID(roomJIDStr) != "" {
		return nil
	}
	err := x.joinMUCNick(ctx, roomJIDStr, nil, false, "")
	telemetry.Fail(span, err)
	return err
}

// joinMUC sends the join presence without any bookmark write (auto-rejoin, imports, events). hist is the
// optional XEP-0045 history request (nil = none, the room's default history applies).
func (x *xmppClient) joinMUC(ctx context.Context, roomJIDStr string, hist *mucHistory) error {
	return x.joinMUCOpts(ctx, roomJIDStr, hist, false)
}

// joinMUCOpts is joinMUC; writeBookmark marks the join chain so that its confirmation writes the bookmark.
func (x *xmppClient) joinMUCOpts(ctx context.Context, roomJIDStr string, hist *mucHistory, writeBookmark bool) error {
	return x.joinMUCNick(ctx, roomJIDStr, hist, writeBookmark, "")
}

// joinMUCNick is joinMUCOpts; a non-empty nickOverride is joined with instead of the nick choice (the
// base for 409 alternatives still comes from the nick choice).
func (x *xmppClient) joinMUCNick(ctx context.Context, roomJIDStr string, hist *mucHistory, writeBookmark bool, nickOverride string) error {
	roomJID, err := jid.Parse(roomJIDStr)
	if err != nil {
		return fmt.Errorf("invalid MUC JID %q: %w", roomJIDStr, err)
	}
	base, nick := x.mucNickChoice(ctx, roomJIDStr)
	if nickOverride != "" {
		nick = nickOverride
	}
	if _, err := roomJID.WithResource(nick); err != nil {
		return fmt.Errorf("MUC JID with resource: %w", err)
	}
	// Track pending join so presence handler can store our occupant JID (and detect 409).
	x.startMUCJoinTrack(roomJIDStr, base, nick, hist, writeBookmark)
	// Watchdog entry before the send, so a fast answer on the Serve goroutine always finds it.
	created := x.noteMUCJoinSent(roomJIDStr, nickOverride, writeBookmark, time.Now())
	if err := x.sendMUCJoin(ctx, roomJIDStr, nick, hist); err != nil {
		x.dropMUCJoinTrack(roomJIDStr)
		if created {
			x.dropMUCJoinWant(roomJIDStr)
		}
		return err
	}
	x.logFor(ctx).Info().
		Str("room", roomJIDStr).
		Str("nick", nick).
		Msg("Sent MUC join presence")
	return nil
}

// mucJoinTokens is the <x xmlns='http://jabber.org/protocol/muc'/> payload of a join presence: the room
// password (from the room's bookmark; never logged) and the optional history request.
func mucJoinTokens(hist *mucHistory, password string) []xml.Token {
	mucX := xml.StartElement{Name: xml.Name{Space: "http://jabber.org/protocol/muc", Local: "x"}}
	toks := []xml.Token{mucX}
	if password != "" {
		pw := xml.StartElement{Name: xml.Name{Local: "password"}}
		toks = append(toks, pw, xml.CharData(password), pw.End())
	}
	if hist != nil {
		h := xml.StartElement{Name: xml.Name{Local: "history"}, Attr: hist.attrs()}
		toks = append(toks, h, h.End())
	}
	return append(toks, mucX.End())
}

// sendMUCJoin sends the XEP-0045 join presence for room/nick (409 retries come through here too).
func (x *xmppClient) sendMUCJoin(ctx context.Context, roomJIDStr, nick string, hist *mucHistory) error {
	if x.mucJoinHistHook != nil {
		x.mucJoinHistHook(roomJIDStr, hist)
	}
	toks := mucJoinTokens(hist, x.bookmarkPassword(roomJIDStr))
	if x.mucJoinTokensHook != nil {
		x.mucJoinTokensHook(roomJIDStr, toks)
	}
	if x.mucSendJoinHook != nil {
		return x.mucSendJoinHook(roomJIDStr, nick)
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("not connected to XMPP")
	}
	roomJID, err := jid.Parse(roomJIDStr)
	if err != nil {
		return fmt.Errorf("invalid MUC JID %q: %w", roomJIDStr, err)
	}
	toJID, err := roomJID.WithResource(nick)
	if err != nil {
		return fmt.Errorf("MUC JID with resource: %w", err)
	}
	p := stanza.Presence{To: toJID}.Wrap(&sliceTokenReader{toks: toks})
	if err := sess.Send(ctx, p); err != nil {
		return fmt.Errorf("send MUC join presence: %w", err)
	}
	return nil
}

// getMUCOurOccupantJID returns our full occupant JID (room@service/nick) for the given MUC room if known.
func (x *xmppClient) getMUCOurOccupantJID(roomBare string) string {
	x.mucOurOccupantJIDMu.RLock()
	defer x.mucOurOccupantJIDMu.RUnlock()
	if x.mucOurOccupantJID == nil {
		return ""
	}
	return x.mucOurOccupantJID[roomBare]
}

// LeaveMUC sends a MUC leave presence (XEP-0045): unavailable to room/nick.
// Uses our stored occupant nick when known (e.g. if we joined with nick_1 due to 409), otherwise preferred/fallback.
func (x *xmppClient) LeaveMUC(ctx context.Context, roomJIDStr string) error {
	ctx, span := telemetry.Start(ctx, "xmpp.muc.leave", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.room.jid", roomJIDStr))
	}
	err := x.leaveMUC(ctx, roomJIDStr)
	if err == nil {
		// An explicit leave: do not bring the room back on the next connect.
		if ferr := x.setMUCLeft(ctx, roomJIDStr, true); ferr != nil {
			x.logFor(ctx).Warn().Err(ferr).Str("room", roomJIDStr).Msg("Failed to persist MUC left flag; the room may be re-joined on reconnect")
		}
		// Keep the bookmark with autojoin off (never fails the leave, never creates one).
		x.bookmarkAfterLeave(ctx, roomJIDStr)
	}
	telemetry.Fail(span, err)
	return err
}

func (x *xmppClient) leaveMUC(ctx context.Context, roomJIDStr string) error {
	if x.mucLeaveHook != nil {
		return x.mucLeaveHook(roomJIDStr)
	}
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return fmt.Errorf("not connected to XMPP")
	}
	roomJID, err := jid.Parse(roomJIDStr)
	if err != nil {
		return fmt.Errorf("invalid MUC JID %q: %w", roomJIDStr, err)
	}
	nick := ""
	if occupant := x.getMUCOurOccupantJID(roomJIDStr); occupant != "" {
		if parsed, err := jid.Parse(occupant); err == nil {
			nick = parsed.Resourcepart()
		}
	}
	if nick == "" {
		nick = x.getPreferredMUCNick(ctx, roomJIDStr)
	}
	toJID, err := roomJID.WithResource(nick)
	if err != nil {
		return fmt.Errorf("MUC JID with resource: %w", err)
	}
	p := stanza.Presence{To: toJID, Type: stanza.PresenceType("unavailable")}.Wrap(nil)
	if err := sess.Send(ctx, p); err != nil {
		return fmt.Errorf("send MUC leave presence: %w", err)
	}
	x.mucOurOccupantJIDMu.Lock()
	if x.mucOurOccupantJID != nil {
		delete(x.mucOurOccupantJID, roomJIDStr)
	}
	x.mucOurOccupantJIDMu.Unlock()
	// A join still pending must not record an occupant JID (or write a bookmark) after this leave.
	x.dropMUCJoinTrack(roomJIDStr)
	x.dropMUCJoinWant(roomJIDStr)
	x.mucAv.clearRoom(roomJID.Bare().String())
	x.logFor(ctx).Info().
		Str("room", roomJIDStr).
		Str("nick", nick).
		Msg("Sent MUC leave presence")
	return nil
}

// HandleMatrixViewingChat is called when a Matrix user views a portal room.
// Aligned with Signal bridge: syncs ghost info and resyncs stale portals.
func (x *xmppClient) HandleMatrixViewingChat(ctx context.Context, msg *bridgev2.MatrixViewingChat) error {
	if msg.Portal == nil || msg.Portal.MXID == "" {
		return nil
	}
	ctx, span := telemetry.Start(ctx, "matrix.handle_viewing_chat", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	log := x.logFor(ctx)
	if span.IsRecording() {
		span.SetAttributes(attribute.String("matrix.room_id", msg.Portal.MXID.String()))
		setPortalTypeAttr(span, msg.Portal)
	}

	// For MUC portals, join the room on XMPP so we can send and receive groupchat.
	if x.portalIsMUC(ctx, msg.Portal) {
		if err := x.joinMUCForPortal(ctx, string(msg.Portal.ID)); errors.Is(err, errMUCLeft) {
			log.Debug().Str("portal_id", string(msg.Portal.ID)).Msg("Not joining MUC on view: the room was left")
		} else if err != nil {
			log.Warn().Err(err).Str("portal_id", string(msg.Portal.ID)).Msg("Failed to join MUC on view; messages may not send until joined")
		}
	}

	// Ensure the user is joined to the portal room (like Signal bridge behavior)
	if x.login != nil && x.login.User != nil {
		userIntent := x.login.User.DoublePuppet(ctx)
		if userIntent == nil {
			// If double-puppet isn't available, try using the ghost intent
			ghost, err := x.conn.br.GetGhostByID(ctx, networkid.UserID(x.loginID))
			if err == nil && ghost != nil {
				userIntent = ghost.Intent
			}
		}
		if userIntent != nil {
			if err := userIntent.EnsureJoined(ctx, msg.Portal.MXID); err != nil {
				log.Warn().Err(err).Str("room_id", msg.Portal.MXID.String()).Msg("Failed to ensure user is joined to portal room")
			}
		}
	}

	// Sync the other user's ghost in DMs (aligned with Signal bridge)
	if msg.Portal.OtherUserID != "" {
		ghost, err := x.conn.br.GetExistingGhostByID(ctx, msg.Portal.OtherUserID)
		if err != nil {
			log.Warn().Err(err).Str("other_user_id", string(msg.Portal.OtherUserID)).Msg("Failed to get ghost for sync")
		} else if ghost != nil {
			meta, _ := ghost.Metadata.(*GhostMetadata)
			if meta != nil && meta.ProfileFetchedAt.Time.Add(5*time.Minute).Before(time.Now()) {
				// Reset profile fetched time for immediate sync
				meta.ProfileFetchedAt.Time = time.Time{}
				info, err := x.GetUserInfo(ctx, ghost)
				if err != nil {
					log.Warn().Err(err).Str("ghost_id", string(ghost.ID)).Msg("Failed to get user info for ghost sync")
				} else if info != nil {
					ghost.UpdateInfo(ctx, info)
				}
			}
		}
	}

	// Resync the portal if it's stale (aligned with Signal bridge: 24 hour threshold)
	portalMeta, _ := msg.Portal.Metadata.(*PortalMetadata)
	if portalMeta != nil && portalMeta.LastSync.Time.Add(24*time.Hour).Before(time.Now()) {
		// Queue a chat resync event (aligned with Signal bridge pattern)
		x.queueRemoteEvent(ctx, &simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventChatResync,
				PortalKey: msg.Portal.PortalKey,
			},
			GetChatInfoFunc: x.GetChatInfo,
		})
	}

	// Backfill on view: the bridge framework may never call FetchMessages for appservice flows
	// (sync-portal, pm command, etc.). Run MAM historical fetch when user views a DM.
	// Run synchronously so history appears before they can send; respects config.backfill.enabled.
	if x.login != nil && x.conn.backfillConfig != nil && x.conn.backfillConfig.Enabled &&
		!x.portalIsMUC(ctx, msg.Portal) && msg.Portal.ID != "" {
		backfillCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		x.mamBackfillOnView(x.log.WithContext(backfillCtx), string(msg.Portal.ID))
		cancel()
	}

	return nil
}

func (x *xmppClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	ctx, span := telemetry.Start(ctx, "matrix.handle_message", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(
			attribute.String("xmpp.peer.jid", string(msg.Portal.ID)),
			attribute.String("matrix.msgtype", string(msg.Content.MsgType)),
		)
		setPortalTypeAttr(span, msg.Portal)
		if msg.Portal.MXID != "" {
			span.SetAttributes(attribute.String("matrix.room_id", msg.Portal.MXID.String()))
		}
		if msg.Event != nil {
			span.SetAttributes(attribute.String("matrix.event_id", string(msg.Event.ID)))
			if !msg.Event.Mautrix.ReceivedAt.IsZero() {
				span.SetAttributes(attribute.Int64("bridge.queue_delay_ms", time.Since(msg.Event.Mautrix.ReceivedAt).Milliseconds()))
			}
		}
	}
	resp, err := x.handleMatrixMessage(ctx, msg)
	telemetry.Fail(span, err)
	return resp, err
}

func (x *xmppClient) handleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	log := x.logFor(ctx)
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("not connected to XMPP")).WithSendNotice(true)
	}
	toJIDStr := string(msg.Portal.ID)
	if toJIDStr == "" {
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("portal has no remote JID")).WithSendNotice(true)
	}
	toJID, err := jid.Parse(toJIDStr)
	if err != nil {
		warnIfDebugIdentifiers(&x.log)
		log.Debug().Str("portal_id", toJIDStr).Msg("HandleMatrixMessage: invalid portal JID format")
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("invalid portal JID format")).WithSendNotice(true)
	}
	// Ensure this portal is included in MAM catch-up (e.g. Cheogram groups not in roster).
	x.addPortalIDForMAMCatchUp(toJIDStr)
	// Fallback backfill: if HandleMatrixViewingChat is never called (e.g. appservice flow), run
	// MAM historical fetch when user sends first message in a DM. Cooldown 1h per portal.
	// Respects config.backfill.enabled.
	if x.conn.backfillConfig != nil && x.conn.backfillConfig.Enabled && !x.portalIsMUC(ctx, msg.Portal) {
		key := strings.ToLower(toJIDStr)
		x.lastBackfillOnViewMu.Lock()
		if x.lastBackfillOnView == nil {
			x.lastBackfillOnView = make(map[string]time.Time)
		}
		last := x.lastBackfillOnView[key]
		x.lastBackfillOnViewMu.Unlock()
		if time.Since(last) > time.Hour {
			x.lastBackfillOnViewMu.Lock()
			x.lastBackfillOnView[key] = time.Now()
			x.lastBackfillOnViewMu.Unlock()
			// Own root linked to this handler's span; the goroutine must not hold the handler's span.
			origin := ctx
			go func() {
				bctx, bspan := telemetry.StartLinkedRoot(x.log.WithContext(context.Background()), "mam.backfill_on_view", origin,
					trace.WithAttributes(attribute.String("mam.trigger", "send")))
				defer bspan.End()
				x.doMAMBackfillOnView(bctx, bspan, toJIDStr, x.mamBackfillMaxMessages())
			}()
		}
	}

	// For MUC portals, ensure we're in the room before sending (join on view may not have run yet).
	if x.portalIsMUC(ctx, msg.Portal) {
		alreadyJoined := x.getMUCOurOccupantJID(toJIDStr) != ""
		if joinErr := x.joinMUCForPortal(ctx, toJIDStr); errors.Is(joinErr, errMUCLeft) {
			return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("you left this room; use `%s join %s` to rejoin it", x.mucCmdPrefix(), toJIDStr)).WithSendNotice(true)
		} else if joinErr != nil {
			log.Warn().Err(joinErr).Str("portal_id", toJIDStr).Msg("Failed to join MUC before send; message may be rejected")
		} else if !alreadyJoined {
			// Give the MUC server time to process our presence before sending (cross-server MUCs can be slow).
			select {
			case <-ctx.Done():
				return nil, bridgev2.WrapErrorInStatus(ctx.Err()).WithSendNotice(true)
			case <-time.After(1500 * time.Millisecond):
			}
		}
	}

	log.Debug().
		Str("content_body_len", fmt.Sprintf("%d", len(msg.Content.Body))).
		Str("content_formatted_body_len", fmt.Sprintf("%d", len(msg.Content.FormattedBody))).
		Str("content_msgtype", string(msg.Content.MsgType)).
		Bool("has_file", msg.Content.File != nil).
		Msg("DEBUG: HandleMatrixMessage - initial Matrix content values")

	body := msg.Content.Body
	if body == "" && msg.Content.FormattedBody != "" {
		body = plainFromFormattedBody(msg.Content.FormattedBody)
		log.Debug().Msg("DEBUG: Using FormattedBody (converted to text) instead of Body")
	}

	log.Debug().
		Str("resolved_body_len", fmt.Sprintf("%d", len(body))).
		Msg("DEBUG: Body resolved from Matrix content")

	// Fill Content.File/URL from raw event when bridge left them unset (e.g. decrypted media).
	if msg.Event != nil {
		if mc := msg.Event.Content.AsMessage(); mc != nil {
			if msg.Content.File == nil && mc.File != nil {
				msg.Content.File = mc.File
			}
			if msg.Content.URL == "" {
				if mc.URL != "" {
					msg.Content.URL = mc.URL
				} else if mc.File != nil {
					msg.Content.URL = mc.File.URL
				}
			}
			// Fill FileName from raw event if Content.FileName is empty
			if msg.Content.FileName == "" && mc.FileName != "" {
				msg.Content.FileName = mc.FileName
				log.Debug().Msg("DEBUG: Got filename from event content")
			}
		}
	}

	// Aligned with mautrix-signal/pkg/msgconv/from-matrix.go ToSignal: normalize from event type when needed
	// (Signal sets content.MsgType from evt.Type for stickers); then same switch and DownloadMedia(content.URL, content.File).
	if msg.Event.Type == event.EventSticker {
		msg.Content.MsgType = event.MessageType(event.EventSticker.Type)
	}

	// event.MessageEventContent.GetCapMsgType() treats empty MsgType with URL/File as media; normalize so switch sees it.
	// Decrypted events sometimes arrive as generic "m.message" instead of m.image/m.file; treat unknown+media as file.
	hasMedia := msg.Content.URL != "" || msg.Content.File != nil
	isKnownType := msg.Content.MsgType == event.MsgText || msg.Content.MsgType == event.MsgNotice ||
		msg.Content.MsgType == event.MsgEmote || msg.Content.MsgType == event.MsgImage ||
		msg.Content.MsgType == event.MsgVideo || msg.Content.MsgType == event.MsgAudio || msg.Content.MsgType == event.MsgFile
	if hasMedia && !isKnownType {
		msg.Content.MsgType = event.MsgFile
	}

	var messageBody, oobURL string
	switch msg.Content.MsgType {
	case event.MsgText, event.MsgNotice, event.MsgEmote:
		messageBody = body
	case event.MsgImage, event.MsgVideo, event.MsgAudio, event.MsgFile:
		mediaLabel := "file"
		switch msg.Content.MsgType {
		case event.MsgImage:
			mediaLabel = "image"
		case event.MsgVideo:
			mediaLabel = "video"
		case event.MsgAudio:
			mediaLabel = "audio"
		}
		log.Debug().
			Str("msg_type", string(msg.Content.MsgType)).
			Str("media_label", mediaLabel).
			Str("body_len", fmt.Sprintf("%d", len(body))).
			Bool("has_file", msg.Content.File != nil).
			Msg("DEBUG: Processing media message - initial values")

		// Matrix encrypted file content has url in content.file.url only (no top-level url).
		mediaURL := msg.Content.URL
		if mediaURL == "" && msg.Content.File != nil {
			mediaURL = msg.Content.File.URL
		}
		if mediaURL == "" {
			return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("%w: %s has no URL", bridgev2.ErrUnsupportedMessageType, msg.Content.MsgType)).WithSendNotice(true)
		}

		// Extract user-provided caption (if any). For OOB messages, we only include actual user text.
		// Matrix often includes the filename in body for media messages; if body equals filename, treat as no caption.
		userCaption := strings.TrimSpace(body)

		// Get filename for comparison - check Content.FileName first, then try event content
		filename := msg.Content.FileName
		if filename == "" && msg.Event != nil {
			if mc := msg.Event.Content.AsMessage(); mc != nil {
				filename = mc.FileName
			}
		}

		log.Debug().
			Bool("body_equals_filename", userCaption == filename).
			Msg("DEBUG: After trimming body, before filename comparison")

		if filename != "" && userCaption == filename {
			// Body is just the filename (Matrix default), not a user caption
			log.Debug().Msg("DEBUG: Body equals filename, treating as empty caption")
			userCaption = ""
		} else if filename == "" && userCaption != "" {
			// If filename is still empty but body looks like a filename (ends with extension, no spaces),
			// treat it as filename, not caption
			if !strings.Contains(userCaption, " ") && (strings.HasSuffix(userCaption, ".png") ||
				strings.HasSuffix(userCaption, ".jpg") || strings.HasSuffix(userCaption, ".jpeg") ||
				strings.HasSuffix(userCaption, ".gif") || strings.HasSuffix(userCaption, ".webp") ||
				strings.HasSuffix(userCaption, ".mp4") || strings.HasSuffix(userCaption, ".mov") ||
				strings.HasSuffix(userCaption, ".mp3") || strings.HasSuffix(userCaption, ".ogg") ||
				strings.HasSuffix(userCaption, ".pdf") || strings.HasSuffix(userCaption, ".zip") ||
				strings.Contains(userCaption, ".")) {
				log.Debug().Msg("DEBUG: Body looks like filename (has extension, no spaces), treating as empty caption")
				userCaption = ""
			}
		}

		// Fallback caption for when upload fails (includes filename for context)
		fallbackCaption := userCaption
		if fallbackCaption == "" {
			fallbackCaption = "Sent a " + mediaLabel
			if msg.Content.FileName != "" {
				fallbackCaption += ": " + msg.Content.FileName
			}
		}
		log.Debug().
			Str("user_caption_len", fmt.Sprintf("%d", len(userCaption))).
			Msg("DEBUG: Caption processing complete")
		// Download media and re-upload to our server (XEP-0363) so Jabber users get a usable HTTPS URL.
		dlCtx, dlSpan := telemetry.StartChild(ctx, "matrix.download_media")
		data, err := x.conn.br.Bot.DownloadMedia(dlCtx, mediaURL, msg.Content.File)
		if err != nil {
			telemetry.Fail(dlSpan, err)
		} else if dlSpan.IsRecording() {
			dlSpan.SetAttributes(attribute.Int("file.size", len(data)))
		}
		dlSpan.End()
		if err != nil {
			log.Warn().Err(err).Str("mxc", string(mediaURL)).Msg("Media download failed; sending MXC link anyway")
			messageBody = fallbackCaption + " " + string(mediaURL)
		} else {
			filename := msg.Content.FileName
			if filename == "" {
				filename = "file"
			}
			contentType := "application/octet-stream"
			if msg.Event != nil {
				if mc := msg.Event.Content.AsMessage(); mc != nil && mc.Info != nil && mc.Info.MimeType != "" {
					contentType = mc.Info.MimeType
				}
			}
			// Use sender's (our) domain for XEP-0363: we upload to our server, then send the GET URL.
			origin, err := jid.Parse(string(x.loginID))
			if err != nil {
				log.Warn().Err(err).Str("login_id", string(x.loginID)).Msg("Failed to parse loginID for upload JID; sending MXC link")
				messageBody = fallbackCaption + " " + string(mediaURL)
			} else {
				uploadJID := x.getUploadJID(ctx, sess, origin.Domain().String())
				if uploadJID == nil {
					log.Warn().Str("domain", origin.Domain().String()).Msg("Could not discover upload service; sending MXC link")
					messageBody = fallbackCaption + " " + string(mediaURL)
				} else {
					log.Debug().Str("upload_jid", uploadJID.String()).Int("size", len(data)).Str("content_type", contentType).Msg("Requesting XEP-0363 upload slot")
					slotCtx, slotSpan := telemetry.StartChild(ctx, "xmpp.iq.upload_slot", trace.WithSpanKind(trace.SpanKindClient))
					if slotSpan.IsRecording() {
						slotSpan.SetAttributes(
							attribute.String("xmpp.iq.namespace", "urn:xmpp:http:upload:0"),
							attribute.Int("file.size", len(data)),
							attribute.String("file.mime", contentType),
						)
					}
					slot, upErr := upload.GetSlot(slotCtx, upload.File{Name: filename, Size: len(data), Type: contentType}, *uploadJID, sess)
					telemetry.Fail(slotSpan, upErr)
					slotSpan.End()
					if upErr != nil {
						log.Warn().Err(upErr).Str("upload_jid", uploadJID.String()).Str("mxc", string(mediaURL)).Msg("XEP-0363 slot request failed; sending MXC link anyway")
						messageBody = fallbackCaption + " " + string(mediaURL)
					} else {
						log.Debug().Msg("Got XEP-0363 slot, uploading file")
						putReq, putErr := slot.Put(ctx, bytes.NewReader(data))
						if putErr != nil {
							log.Warn().Err(putErr).Str("mxc", string(mediaURL)).Msg("XEP-0363 PUT build failed; sending MXC link anyway")
							messageBody = fallbackCaption + " " + string(mediaURL)
						} else {
							client := &http.Client{Timeout: 60 * time.Second}
							// Span carries host/method only: the PUT URL is a capability URL and is never recorded.
							_, putSpan := telemetry.StartChild(ctx, "http.upload_put", trace.WithSpanKind(trace.SpanKindClient))
							if putSpan.IsRecording() {
								putSpan.SetAttributes(telemetry.HTTPAttrs(putReq)...)
								putSpan.SetAttributes(attribute.Int("file.size", len(data)))
							}
							resp, doErr := client.Do(putReq)
							if doErr != nil {
								telemetry.Fail(putSpan, redactURLError(doErr))
							} else if putSpan.IsRecording() {
								putSpan.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
							}
							putSpan.End()
							if doErr != nil {
								log.Warn().Err(redactURLError(doErr)).Str("mxc", string(mediaURL)).Msg("XEP-0363 upload PUT failed; sending MXC link anyway")
								messageBody = fallbackCaption + " " + string(mediaURL)
							} else {
								resp.Body.Close()
								if resp.StatusCode < 200 || resp.StatusCode >= 300 {
									log.Warn().Int("status", resp.StatusCode).Str("mxc", string(mediaURL)).Msg("XEP-0363 upload returned error status; sending MXC link anyway")
									messageBody = fallbackCaption + " " + string(mediaURL)
								} else {
									getURLStr := slot.GetURL.String()
									// When OOB is present, use user caption only (empty if none) so client displays media inline
									messageBody = userCaption
									oobURL = getURLStr
									log.Info().
										Str("user_caption_len", fmt.Sprintf("%d", len(userCaption))).
										Str("message_body_len", fmt.Sprintf("%d", len(messageBody))).
										Msg("DEBUG: XEP-0363 upload successful, final values before sending")
								}
							}
						}
					}
				}
			}
		}
	default:
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("%w: %s", bridgev2.ErrUnsupportedMessageType, msg.Content.MsgType)).WithSendNotice(true)
	}

	// Handle reply (XEP-0461): msg.ReplyTo is already a *database.Message (resolved by bridgev2).
	// In a MUC the reply id is the target's room stanza-id; in 1:1 chats the id we sent or the row ID.
	isMUCPortal := x.portalIsMUC(ctx, msg.Portal)
	var replyID, replyToJID string
	if msg.ReplyTo != nil {
		ref, roomRef, ok := replyRefForMessage(msg.ReplyTo, isMUCPortal)
		switch {
		case !ok:
			log.Debug().Msg("Reply target has no XMPP id; sending without XEP-0461 reply")
		default:
			replyID = ref
			if isMUCPortal && !roomRef {
				log.Debug().Msg("Reply target has no room stanza-id; falling back to its message id")
			}
			if senderIDStr := string(msg.ReplyTo.SenderID); senderIDStr != "" {
				replyToJID = senderIDStr
			}
			log.Debug().Bool("muc", isMUCPortal).Bool("room_stanza_id", roomRef).Bool("has_to", replyToJID != "").Msg("Reply target resolved")
		}
	}

	// Use groupchat for MUC portals; use chat for 1:1 DMs (matches Cheogram and standard 1:1).
	msgType := stanza.ChatMessage
	if isMUCPortal {
		msgType = stanza.MessageType("groupchat")
	}
	// MUC: pick the stanza id and register it for the echo rendezvous BEFORE sending, so a fast echo
	// from the room cannot be missed (xmpp_muc_echo.go).
	var preID string
	if isMUCPortal {
		var idErr error
		preID, idErr = generateXMPPMessageID()
		if idErr != nil {
			return nil, bridgev2.WrapErrorInStatus(idErr).WithSendNotice(true)
		}
		x.mucEcho.register(toJIDStr, preID)
	}
	log.Debug().
		Str("final_message_body_len", fmt.Sprintf("%d", len(messageBody))).
		Str("final_oob_url_len", fmt.Sprintf("%d", len(oobURL))).
		Str("reply_id", replyID).
		Str("reply_to_jid", replyToJID).
		Str("to_jid", toJID.String()).
		Str("msg_type", string(msgType)).
		Msg("DEBUG: About to call sendXMPPMessageWithOOB")

	outMsg := x.withSignalFlags(outMessage{To: toJID, Type: msgType, ID: preID, Body: messageBody, OOBURL: oobURL, ReplyID: replyID, ReplyTo: replyToJID})
	xmppMsgID, err := sendXMPPOut(ctx, sess, outMsg)
	if err == nil {
		x.noteOutboundSent(outMsg)
	}
	if err != nil {
		if preID != "" {
			x.mucEcho.discard(toJIDStr, preID)
		}
		return nil, bridgev2.WrapErrorInStatus(fmt.Errorf("failed to send XMPP message: %w", err)).WithSendNotice(true)
	}

	log.Debug().
		Str("xmpp_msg_id", xmppMsgID).
		Str("matrix_event_id", string(msg.Event.ID)).
		Msg("DEBUG: sendXMPPMessageWithOOB completed successfully, storing XMPP message ID in metadata")
	now := time.Now()
	id := networkid.MessageID(fmt.Sprintf("matrix:%s", msg.Event.ID))

	// Store XMPP message ID in metadata so replies to Matrix-originated messages work
	msgMeta := &MessageMetadata{
		XMPPMessageID: xmppMsgID,
		Media:         oobURL != "",
	}
	log.Debug().
		Str("matrix_event_id", string(msg.Event.ID)).
		Str("xmpp_msg_id", xmppMsgID).
		Str("db_message_id", string(id)).
		Msg("DEBUG: Stored XMPP message ID in metadata for Matrix-originated message")

	// Send read receipt from bridge bot only when matrix.delivery_receipts is enabled.
	if x.conn.deliveryReceipts && msg.Portal != nil && msg.Portal.MXID != "" && msg.Event != nil && msg.Event.ID != "" {
		if err := x.conn.br.Bot.MarkRead(ctx, msg.Portal.MXID, msg.Event.ID, now); err != nil {
			log.Warn().Err(err).Str("room_id", msg.Portal.MXID.String()).Str("event_id", string(msg.Event.ID)).Msg("Failed to send read receipt for bridged message")
		} else {
			log.Debug().Str("event_id", string(msg.Event.ID)).Msg("Sent read receipt for bridged message")
		}
	}

	resp := &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			Room:      msg.Portal.PortalKey,
			SenderID:  networkid.UserID(x.loginID),
			ID:        id,
			Timestamp: now,
			SendTxnID: msg.InputTransactionID,
			Metadata:  msgMeta,
		},
	}
	if preID != "" {
		resp.PostSave = x.mucEchoPostSave(toJIDStr, preID)
	}
	return resp, nil
}

// PreHandleMatrixReaction prepares the reaction for sending to XMPP.
// In Jabber, reactions are sent as replies with just the emoji character.
func (x *xmppClient) PreHandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	// Extract emoji from the reaction key (e.g., "👍" from the reaction event)
	emoji := msg.Content.RelatesTo.Key
	x.log.Debug().
		Str("target_message_id", string(msg.TargetMessage.ID)).
		Msg("DEBUG: PreHandleMatrixReaction")
	return bridgev2.MatrixReactionPreResponse{
		SenderID: networkid.UserID(x.loginID),
		EmojiID:  "",
		Emoji:    emoji,
	}, nil
}

// HandleMatrixReaction sends a Matrix reaction to XMPP using XEP-0444 Message Reactions format.
// Reactions are sent as <reactions> elements with the target message ID and emoji.
func (x *xmppClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	ctx, span := telemetry.Start(ctx, "matrix.handle_reaction", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.String("xmpp.peer.jid", string(msg.Portal.ID)))
		if msg.Portal.MXID != "" {
			span.SetAttributes(attribute.String("matrix.room_id", msg.Portal.MXID.String()))
		}
		if msg.Event != nil {
			span.SetAttributes(attribute.String("matrix.event_id", string(msg.Event.ID)))
		}
	}
	resp, err := x.handleMatrixReaction(ctx, msg)
	telemetry.Fail(span, err)
	return resp, err
}

func (x *xmppClient) handleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	log := x.logFor(ctx)
	x.mu.Lock()
	sess := x.session
	x.mu.Unlock()
	if sess == nil {
		return nil, fmt.Errorf("not connected to XMPP")
	}

	toJIDStr := string(msg.Portal.ID)
	if toJIDStr == "" {
		return nil, fmt.Errorf("portal has no remote JID")
	}
	toJID, err := jid.Parse(toJIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid portal JID format: %w", err)
	}

	// XEP-0444 target id: the room stanza-id in a MUC when known, otherwise the id we sent
	// (Matrix-originated rows) or the row ID.
	isMUCPortal := x.portalIsMUC(ctx, msg.Portal)
	targetMessageID, roomRef, ok := replyRefForMessage(msg.TargetMessage, isMUCPortal)
	if !ok {
		return nil, fmt.Errorf("target message has no XMPP message ID")
	}
	if isMUCPortal && !roomRef {
		log.Debug().Msg("Reaction target has no room stanza-id; falling back to its message id")
	}

	if targetMessageID == "" {
		return nil, fmt.Errorf("could not determine XMPP message ID for reaction target")
	}

	// Send the reaction using XEP-0444 format. Multiple reactions per user per message:
	// we only send the new emoji here, so Jabber shows one reaction per user (consistent with
	// Jabber→Matrix). Future: see README "To-do (unimplemented work)" — multiple reactions per user per message.
	emoji := msg.PreHandleResp.Emoji
	log.Debug().
		Str("target_message_id", targetMessageID).
		Msg("DEBUG: Sending Matrix reaction to XMPP using XEP-0444 format")

	// Use groupchat for MUC portals; use chat for 1:1 DMs (matches regular messages and standard 1:1).
	msgType := stanza.ChatMessage
	if isMUCPortal {
		msgType = stanza.MessageType("groupchat")
	}
	// MUC: register the stanza id before sending so the room's echo is recognised as ours.
	var preID string
	if isMUCPortal {
		var idErr error
		preID, idErr = generateXMPPMessageID()
		if idErr != nil {
			return nil, fmt.Errorf("failed to generate XMPP message ID: %w", idErr)
		}
		x.mucEcho.register(toJIDStr, preID)
	}
	_, err = sendXMPPReactionsID(ctx, sess, toJID, targetMessageID, []string{emoji}, msgType, preID)
	if err != nil {
		if preID != "" {
			x.mucEcho.discard(toJIDStr, preID)
		}
		return nil, fmt.Errorf("failed to send reaction to XMPP: %w", err)
	}

	return &database.Reaction{}, nil
}

// HandleMatrixReactionRemove removes a Matrix reaction by sending an empty reply or deleting the reaction message.
// In Jabber, we can't easily "remove" a reaction, but we could send an empty reply or just log it.
// For now, we'll just return success (the reaction message in Jabber will remain).
func (x *xmppClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	x.log.Debug().
		Str("target_message_id", string(msg.TargetReaction.MessageID)).
		Msg("DEBUG: HandleMatrixReactionRemove - Jabber doesn't support removing reactions, ignoring")
	// Jabber doesn't have a standard way to remove reactions, so we just return success
	// The original reaction message will remain in Jabber
	return nil
}

// getUploadJID discovers the XEP-0363 upload service JID for the given domain.
// It tries common subdomains (upload, share, files) and caches the successful result.
func (x *xmppClient) getUploadJID(ctx context.Context, sess *xmpp.Session, domain string) *jid.JID {
	x.uploadJIDMu.Lock()
	defer x.uploadJIDMu.Unlock()
	// Return cached result if available
	if x.uploadJID != nil {
		return x.uploadJID
	}
	// Try common subdomains used by XMPP servers for file upload
	commonSubdomains := []string{"upload", "share", "files", "http_upload"}
	ctx, span := telemetry.StartChild(ctx, "xmpp.upload.discover")
	defer span.End()
	log := x.logFor(ctx)
	span.SetAttributes(attribute.Int("candidate.count", len(commonSubdomains)))
	for _, subdomain := range commonSubdomains {
		candidate := subdomain + "." + domain
		uploadJID, err := jid.Parse(candidate)
		if err != nil {
			continue
		}
		// Try a quick slot request to see if this service exists
		// Use a minimal test request (1 byte file) with a short timeout
		testCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = upload.GetSlot(testCtx, upload.File{Name: "test", Size: 1, Type: "application/octet-stream"}, uploadJID, sess)
		cancel()
		if err == nil {
			// Success! Cache this JID
			x.uploadJID = &uploadJID
			log.Info().Str("upload_jid", uploadJID.String()).Msg("Discovered XEP-0363 upload service")
			return x.uploadJID
		}
		// Log debug info but continue trying other subdomains
		span.AddEvent("candidate_failed", trace.WithAttributes(attribute.String("server.address", candidate)))
		log.Debug().Err(err).Str("candidate", candidate).Msg("Upload service discovery attempt failed")
	}
	// None of the common subdomains worked
	span.SetStatus(codes.Error, "no upload service found")
	log.Warn().Str("domain", domain).Msg("Could not discover upload service via common subdomains")
	return nil
}
