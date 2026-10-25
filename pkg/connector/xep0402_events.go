package connector

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// Live bookmark changes (PEP events from our own account). The Serve goroutine only parses and queues
// (handlePubSubEvent); one worker goroutine per session applies them here. The bridge never writes in
// reaction to an event, so no loop can form: the cache compare ignores our own publish echoes and every
// action is idempotent against the current state.
//
//   - autojoin turned on (a transition, not any edit of an already-on bookmark): clear the left flag,
//     join, and create the Matrix room when there is none (per-session import budget);
//   - autojoin turned off, or the bookmark retracted, while we are in the room: leave the MUC, mark the
//     room left, KEEP the portal, post a notice. Viewing or sending in the Matrix room does not rejoin
//     (joinMUCForPortal); only the join command does;
//   - a node purge/delete is only logged.

// runBookmarkEventWorker applies queued events until the session ends. ch is the session's channel
// (bookmarkState.reset creates a new one per session, so a stale worker never reads a newer session).
func (x *xmppClient) runBookmarkEventWorker(sessCtx context.Context, ch <-chan bookmarkBatch) {
	for {
		select {
		case <-sessCtx.Done():
			return
		case b := <-ch:
			x.applyBookmarkBatch(sessCtx, b)
		}
	}
}

// applyBookmarkBatch applies one event's changes in order, staggered like the startup joins. It runs in
// the worker goroutine under its own root span.
func (x *xmppClient) applyBookmarkBatch(sessCtx context.Context, b bookmarkBatch) {
	ctx, span := telemetry.Start(sessCtx, "xmpp.bookmarks.event", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("xmpp.bookmarks.changes", len(b.changes)))
	}
	acted := 0
	for _, ch := range b.changes {
		if ctx.Err() != nil {
			return
		}
		if x.applyBookmarkChange(ctx, ch) {
			acted++
			if !waitCtx(ctx, mucRejoinInterval) {
				return
			}
		}
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("xmpp.bookmarks.acted", acted))
	}
}

// applyBookmarkChange applies one change; true when it did network work (so the caller staggers).
func (x *xmppClient) applyBookmarkChange(ctx context.Context, ch bookmarkChange) bool {
	log := x.logFor(ctx)
	switch {
	case ch.Purge:
		log.Info().Msg("Bookmarks: the bookmarks node was purged or deleted; nothing is changed")
		return false
	case ch.Retract:
		key := bookmarkKey(ch.ID)
		x.bm.cacheDelete(key)
		return x.remoteLeave(ctx, key)
	case ch.Conf == nil:
		return false
	}
	key := bookmarkKey(ch.ID)
	conf := *ch.Conf
	prev, hadPrev, _ := x.bm.lookup(key)
	if hadPrev && prev.equal(conf) {
		log.Debug().Str("room", key).Msg("Bookmarks: event equals the cached bookmark (our own echo); ignored")
		return false
	}
	x.bm.cacheSet(key, conf)
	if !conf.Autojoin {
		return x.remoteLeave(ctx, key)
	}
	if hadPrev && prev.Autojoin {
		// An edit of a bookmark that already was on (name, nick, ...): not a join request.
		return false
	}
	return x.remoteJoin(ctx, key)
}

// mucJoinInFlight reports whether a join for the room is pending (sent, not yet confirmed) or still
// wanted by the join watchdog (e.g. after a transient join error; muc_join_health.go).
func (x *xmppClient) mucJoinInFlight(room string) bool {
	x.mucPendingJoinMu.Lock()
	defer x.mucPendingJoinMu.Unlock()
	_, ok := x.mucPendingJoin[room]
	return ok || x.mucJoinWant[room] != nil
}

// remoteLeave honors a bookmark turned off (or deleted) on another client. A room we are not in and have
// no active portal for is a no-op (no portal is ever created here).
func (x *xmppClient) remoteLeave(ctx context.Context, room string) bool {
	log := x.logFor(ctx)
	portal, err := x.lookupMUCPortal(ctx, room)
	if err != nil {
		log.Warn().Err(err).Str("room", room).Msg("Bookmarks: portal lookup failed; leave not applied")
		return false
	}
	inRoom := x.getMUCOurOccupantJID(room) != "" || x.mucJoinInFlight(room)
	active := false
	if portal != nil && portal.MXID != "" {
		meta, _ := portal.Metadata.(*PortalMetadata)
		active = meta == nil || !meta.MUCLeft
	}
	if !inRoom && !active {
		log.Debug().Str("room", room).Msg("Bookmarks: bookmark is off but we are not in the room; nothing to do")
		return false
	}
	if inRoom {
		if err := x.leaveMUC(ctx, room); err != nil {
			log.Warn().Err(err).Str("room", room).Msg("Bookmarks: leaving the room failed; will reconcile on the next connect")
			return false
		}
	}
	if err := x.setMUCLeft(ctx, room, true); err != nil {
		log.Warn().Err(err).Str("room", room).Msg("Bookmarks: failed to persist the left flag")
	}
	x.notifyMUC(ctx, room, bookmarkLeftNotice(x.mucCmdPrefix(), room))
	log.Info().Str("room", room).Msg("Bookmarks: left room because its bookmark was turned off on another client (portal kept)")
	return true
}

// remoteJoin honors a bookmark turned on on another client.
func (x *xmppClient) remoteJoin(ctx context.Context, room string) bool {
	log := x.logFor(ctx)
	if !bookmarkRoomValid(room) {
		return false
	}
	portal, err := x.lookupMUCPortal(ctx, room)
	if err != nil {
		log.Warn().Err(err).Str("room", room).Msg("Bookmarks: portal lookup failed; join not applied")
		return false
	}
	if portal != nil && portal.MXID != "" {
		if meta, _ := portal.Metadata.(*PortalMetadata); meta != nil && meta.MUCLeft {
			// Turned on elsewhere = an explicit join: the room comes back.
			if err := x.setMUCLeft(ctx, room, false); err != nil {
				log.Warn().Err(err).Str("room", room).Msg("Bookmarks: failed to clear the left flag")
			}
		}
		if x.getMUCOurOccupantJID(room) != "" || x.mucJoinInFlight(room) {
			return false
		}
		hist := mucRejoinHistory(x.newestBridgedMessage(ctx, room), time.Now())
		if err := x.joinMUC(ctx, room, hist); err != nil {
			log.Warn().Err(err).Str("room", room).Msg("Bookmarks: join after bookmark change failed")
			return false
		}
		log.Info().Str("room", room).Msg("Bookmarks: joined room because its bookmark was turned on on another client")
		return true
	}
	if x.bm.takeImportBudget(1) == 0 {
		log.Warn().Str("room", room).Msg("Bookmarks: import budget reached; room not imported")
		if x.bm.markBudgetNoticed() {
			x.notifyManagement(ctx, bookmarkBudgetNotice(x.mucCmdPrefix()))
		}
		return false
	}
	if err := x.importBookmarkedRoom(ctx, room); err != nil {
		log.Warn().Err(err).Str("room", room).Msg("Bookmarks: importing a new bookmarked room failed")
		return false
	}
	log.Info().Str("room", room).Msg("Bookmarks: imported room from a new bookmark")
	return true
}
