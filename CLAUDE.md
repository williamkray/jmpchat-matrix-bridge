# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

jmpchat-matrix-bridge (formerly mautrix-jabber) is a Matrix-Jabber (XMPP) puppeting bridge written in Go, built on the **mautrix bridgev2** framework, with a focus on JMP.chat/Cheogram. It is also a generic XMPP bridge: bidirectional DM and MUC bridging, media file transfers, reactions, replies, message carbons, MAM backfill, and Cheogram SMS/MMS integration.

## Build

Builds use Docker to ensure a repeatable environment with all C dependencies (CGO, libolm, libopus, sqlite-dev):

```bash
./build.sh
```

This runs `docker build` (tags `sipcalls nolibopusfile`, so SIP calls are included) and extracts the binary. For a local build without encryption support (requires Go 1.26+, CGO, sqlite-dev):

```bash
go build -tags nocrypto -ldflags "-s -w" -o jmpchat-matrix-bridge ./cmd/jmpchat-matrix-bridge
```

To include calls in a local build (also needs libopus dev headers and pkg-config): `go build -tags "nocrypto sipcalls nolibopusfile" ...`.

The production build includes libolm for Matrix E2EE. Unit tests run with `go test -tags nocrypto ./...`; there is no lint configuration.

## Naming

The project was renamed from mautrix-jabber to jmpchat-matrix-bridge (issue #9; module `github.com/williamkray/jmpchat-matrix-bridge`, binary and `cmd/` dir `jmpchat-matrix-bridge`). Kept on purpose: `DBOwner: "megabridge/mautrix-jabber"` in `cmd/jmpchat-matrix-bridge/main.go` (existing databases store that owner and the bridge refuses to start on a mismatch; never change it), the legacy env var `MAUTRIX_JABBER_CHEOGRAM` (still read as a fallback), `jabberCapID` (stored capability ID), `ContentKeyStanzaID` (`me.mssj.mautrixjabber.stanza_id`, key in already-sent event content), `NetworkID` "jabber", prefix `!jabber`, and the `mautrix-jabber-verify.db` local file name. The image keeps a compat symlink `/app/mautrix-jabber`.

## Architecture

All bridge logic lives in `pkg/connector/`. The bridgev2 framework handles Matrix protocol, appservice lifecycle, config, and database.

### Entry Point

- **`cmd/jmpchat-matrix-bridge/main.go`** — Bootstrap. Creates `BridgeMain` with `JabberConnector`, registers custom bot commands (`sync-roster`, `add-to-roster`, `save-contact`, `remove-from-roster`, `search-roster`, `join`, `leave`, `bookmarks-sync`, `muc-nick`, `muc-nick-default`), injects decoded network config via `PostInit`.

### Connector Core

- **`pkg/connector/connector.go`** — `JabberConnector` (implements `bridgev2.NetworkConnector`). Login flow (JID+password via `jabberLoginProcess`), `login password-server` flow (`buildLoginMeta`, per-login server override), `LoadUserLogin` (spawns `xmppClient` or `stubJabberClient`). Also defines helper interfaces (`RosterSyncer`, `RosterManager`, `MUCJoiner`) and MUC nick persistence functions.

- **`pkg/connector/xmpp_dial.go`** — XMPP dialing (adapted in part from mautrix-xmpp, see `LICENSE-THIRD-PARTY`). `dialXMPP` uses the per-login override (`dialOptions`, `parseServerOverride`) or SRV discovery (`resolveXMPPEndpoints`: `_xmpps-client` then `_xmpp-client`, priority/direct-TLS-first/weight order, `.` target = unavailable, max 8, fallback `domain:5222`), tries endpoints in order and stops on a SASL failure. Direct TLS (XEP-0368) and STARTTLS both verify the certificate against the JID domain, never the SRV target or override host. Override hosts and SRV targets on ports other than 5222/5223 that resolve to private/loopback addresses are refused unless `network.allow_private_server_override` (`errPrivateSRV`); each endpoint has its own `dialEndpointTimeout`; login errors never echo dial error text; the plain re-login flow dials with the existing login's stored override (`existingDialOptions`). `saslCondition` reads mellium's internal `saslerr.Error` by reflection (depends on mellium internals). Seams: `xmppResolver`, `dialXMPPFunc`, `dialEndpointFunc`, `xmppTLSConfigHook`.

- **`pkg/connector/xmpp_conn_health.go`** — connection health. `classifyAuth` (via `saslCondition`) feeds `handleDialFailure` in `Connect`: account-disabled, credentials-expired and invalid-authzid are final at once, not-authorized after 2 consecutive results, temporary-auth-failure/aborted stay transient. A final failure sets `xmppClient.authFailed` (only a new client, i.e. re-login, clears it), sends `StateBadCredentials` (`jabber-<condition>`, fixed English text, never the server's SASL text), logs the condition only and posts one management-room notice; `Connect` re-sends the state and returns while `authFailed`, and `ReconnectLoop` returns. TLS verification errors report `StateUnknownError` `jabber-tls-error` and keep retrying. `ReconnectLoop` waits with `reconnectWait` (ctx-aware; `wakeReconnect` from `serveMessages` after a session end wakes it). `runKeepalive` (session goroutine, own span only on timeout): after 90 s without inbound traffic (`lastInbound`, stamped at the top of `messageLoggingHandler.HandleXMPP`) it sends an XEP-0199 ping every 30 s tick; 2 misses close the connection; a separate route check (`routeCheckInterval`, 3 min, independent of traffic) pings our OWN full JID via `routePing` (not `ping.Send`: a stanza error reply, incl. service-unavailable, is a miss; span `xmpp.keepalive.route_timeout`), 2 route misses close it too, never both pings in one tick (`sess.Conn().Close()`, never `sess.Close()`), so Serve returns and the loop reconnects. Server pings are answered by `ping.Handle()` in `newServeHandler` (`urn:xmpp:ping` is in `bridgeFeatureVars`). The route check says nothing about MUC membership (see `muc_join_health.go`). `TestDialSASLFailureClassified` runs a fake direct-TLS server and fails if mellium moves `saslerr.Error`. Test seams: `stateHook`, `mgmtNoticeHook`, `closeConnHook`, `pingFunc`, the `keepalive*`/`reconnect*` timing vars.

- **`pkg/connector/xmpp_client.go`** — `xmppClient` (implements `bridgev2.NetworkAPI` and related interfaces). The largest file (~1800 lines). Handles: reconnect loop with exponential backoff, roster sync, MUC join/leave/nick, `HandleMatrixMessage` (text, media, replies, reactions → XMPP), `ResolveIdentifier` (JID/phone → portal key), `GetChatInfo`/`GetUserInfo`, portal type detection (`portalIsMUC` via disco#info or heuristic).

- **`pkg/connector/muc_nick_conflict.go`** — MUC nick conflict handling: on 409 `onMUCJoinConflict` (Serve goroutine, state only) advances a per-room `mucJoinTrack` (`base_1`..`base_3`), `continueMUCJoinAfterConflict` sends the retry from a goroutine with its own span. Confirmation = the existing pending-nick match in `handlePresence` (our available presence from `room/nick`), or our own presence with status 110 under a different, server-assigned nick (status 210; occupant JID stored, not announced, no bookmark nick); then `announceMUCNickChange` stores `PortalMetadata.MUCAutoNick`/`MUCAutoNickBase` (auto, only valid while the preferred base is unchanged; `muc-nick` clears it, see `chooseMUCNick`) and posts an m.notice to the portal room and the management room. All-taken posts a failure notice.

### Message Handling

- **`pkg/connector/xmpp_inbound.go`** — Inbound message routing. Dispatches incoming XMPP stanzas to DM or MUC handlers, extracts sender, determines portal key, queues `RemoteEvent` for the framework. Handles self-echo filtering for MUC.

- **`pkg/connector/xmpp_message.go`** — Message conversion. Parses XMPP message content (body text, XEP-0066 OOB media URLs, XEP-0461 replies, XEP-0444 reactions, chat state notifications, which `xmpp_signals.go` bridges). Builds `bridgev2.ConvertedMessagePart` for Matrix. Handles deferred stanza ID metadata updates.

### Retractions and moderation (XMPP -> Matrix)

- **`pkg/connector/xmpp_retract_parse.go`** — `retractScan`: XEP-0424/0425 (`:1`) and legacy `:0` detection by resolved namespace on direct children of `<message>` (never the XEP-0353 jingle `<retract>`); the parsers (`parseMessageBodyOOBIDs`, carbons, MAM) fill `messageIDs.Retract`/`Tombstone` and drop the fallback body whenever a retract element is present.
- **`pkg/connector/xmpp_retract.go`** — room-scoped `findMessagesByXMPPRef` (per-kind lookup order). **`xmpp_retract_event.go`** — `retraction`, `validateRetraction` (all sender rules), `newRetractionEvent` (`simplevent.MessageRemove` + `PreHandleWithResultFunc`/`PostHandleFunc`, `retractionEnv` seam), reason path (`removeWithReason`: connector redacts with `Reason` and returns Success; no reason uses the bridgev2 path), the `jabber_retracted:<login>:<lower portal>:<scope>:<ref>` KV tombstones (scope `snd=<sender id>`, `occ=<occupant-id>` or the sender-independent `stanza` for room/archive stanza-ids; hidden MAM originals are tombstoned too) (checked in `jabberRemoteMessage.ConvertMessage` and the MAM paths) and the MAM batch filter.
### Redactions (Matrix -> XMPP)

- **`pkg/connector/xmpp_retract_out.go`** — `HandleMatrixMessageRemove` (`RedactionHandlingNetworkAPI`): own messages only (`isOwnMessage`), Cheogram JIDs (`IsCheogramDMJID`, incl. group MMS) are refused with an `m.notice` and a nil return (redaction stands), DM ref via `xmppIDForMessage` (origin-id, else id; `matrix:` rows use `XMPPMessageID`), MUC ref = room stanza-id only (`resolveRetractRef`), then tombstones via `markRetractedRefs`. Stanza builders `retractionPayload`/`sendXMPPRetraction` are in `xmpp_message.go`.
- **`pkg/connector/xmpp_muc_echo.go`** — `mucEchoTable`: rendezvous between the room's echo of our MUC message (Serve goroutine, `handleOwnMUCEcho`, exact origin-id/id match, memory only) and the row insert (`MatrixMessageResponse.PostSave`); stores `StanzaID`/`OccupantID` on the row; redactions wait up to `mucEchoWait` (3 s). `handleMatrixMessage` registers the id before sending. Issue #11 (own-nick messages from other clients) should reuse `matchEcho`.

- Retraction events never create portals and go through `queueRemoteEvent` (`x.queueHook` is a test seam).

### Corrections (XEP-0308, both ways)

- **`pkg/connector/xmpp_correct.go`** — inbound: `correction` (claim, `<replace id>` target, correction stanza id, body, reply data, sender id) becomes a `simplevent.Message[*correction]` of type `RemoteEventEdit` (`newCorrectionEvent`, `correctionEnv` test seam like `retractionEnv`; `queueRemoteEvent` attaches the lazy trace hooks). The pre-handler (portal event loop, DB allowed) dedupes by the `jabber_corrected:<login>:<lower portal>:<scope>:<correction id>` KV key (value = bridgev2 id of the ORIGINAL; never GC'd), resolves the target with `findMessagesByXMPPRef` (`refCorrectionDM`/`refCorrectionMUC`: `<replace id>` is the original's `id` attribute, never the room stanza-id), falls back to the alias key for a correction of a previous correction, validates with `validateOwnership` (the non-moderation rules of `validateRetraction`, shared via `senderClaim`), then checks tombstones and media (`MessageMetadata.Media` or an OOB url -> ignored) and sets `TargetMessage` + `Sender.ForceEditOrigSender` (ownership is already proven, so bridgev2 uses the original sender's intent). A correction is never bridged as a new message and never creates a portal (unknown target = Ignored). Serve-side entry `handleInboundCorrection` only builds and queues; wired in live DM, live MUC (after the echo check, so the echo of our own correction is dropped by `handleOwnMUCEcho`), both carbon handlers, MAM catch-up/on-view (`queueMAMCorrection`) and batch backfill (`foldBatchCorrections`: folds corrections into originals of the same batch and drops them). `textForBody` (xmpp_inbound.go) is the text conversion shared with messages.
- **`pkg/connector/xmpp_correct_out.go`** — `HandleMatrixEdit` (`EditHandlingNetworkAPI`, `jabberCaps.Edit` fully supported, no max age/count): own text messages only, relay rule as for redactions, always names the original id (`xmppIDForMessage`), MUC joins via `ensureInMUC` and registers the stanza id in `x.mucEcho` first. The sent correction is recorded under the `jabber_corrected:` key (own scope) so its MAM copy/replay is not applied again. Cheogram JIDs (`IsCheogramDMJID`, incl. group MMS) get no `<replace>`: the edit goes out as a NEW plain message with body `"* " + text` (the new id is tombstoned so MAM catch-up does not bridge it back; the Matrix edit stands). Send seam: `outMessage` + `sendXMPPOut` in `xmpp_message.go` (single outbound builder; the old `sendXMPPMessageWithOOB*` wrappers delegate to it; tests use `x.sendOutHook`).
- **`pkg/connector/xmpp_subject_encrypted.go`** — live MUC subject and encrypted messages. `handleGroupchatWithOOB` (after the retraction branch, before the body-less skip and the echo check): `ids.Extras.Subject != nil` with no body/OOB/reaction is a subject change (a subject with a body is a normal message; DM subjects are never read) -> `queueMUCSubject` (Serve goroutine, queueing only; `simplevent.ChatInfoChange` with an empty `Sender` so bridgev2 uses the bridge bot, `CreatePortal` false, control characters stripped by `sanitizeSubject`, length-only logging; bridgev2 ignores an unchanged topic). `composeMUCTopic` (xmpp_client.go) is the single topic rule shared with the disco#info resync: subject when non-empty, else description (a cleared subject empties the topic live; a resync falls back to the description). Encrypted: `encryptedKind` (`Extras.Encryption` omemo/omemo2/openpgp/pgp/eme:*, or a body/OOB starting `aesgcm://`) and `applyEncryption` (clears body and oobURL, only when there is a body or OOB, so key-transport stanzas stay dropped) set `jabberRemoteMessage.encrypted` at live DM, live MUC, both carbons and both MAM catch-up paths; `convertMessage` returns one fixed `m.notice` (`encryptedNotice`, no content, never downloads) before any OOB/URL handling, rows keep their ids; `FetchMessages` uses `mamBackfillContent`. An encrypted correction (`messageExtras.Encryption != ""` on a `<replace>`) is ignored at every path (`handleInboundCorrection` via `correction.encrypted`, `queueMAMCorrection`, `foldBatchCorrectionsDetailed`; debug log reason `encrypted`), never applied as an edit with the fallback text. `handleMatrixMessage` converts an HTML-only `formatted_body` with `plainFromFormattedBody` (`format.HTMLToText`) instead of sending raw HTML.
- **`pkg/connector/xmpp_signals.go`** / **`xmpp_signals_out.go`** — chat signals both ways: XEP-0085 typing, XEP-0333 displayed markers, XEP-0184 delivery receipts. Config (`network.send_typing`, `send_read_markers`, `send_delivery_receipts`, `muc_read_markers`, all default true; `*bool` in `JabberConfig`, resolved by `signalPrefs`; the example config documents the privacy effect, `muc_read_markers` tells room occupants what you read). `signalState` (`xmppClient.sig`, reset in `Connect`) holds the peers that sent a chat state this session (typing is only sent to them), typing and displayed dedupe. Inbound (`handleInboundSignals`, Serve goroutine, memory + queue only; MAM never): a body-less signal whose stanza carries a direct-child XEP-0203 `<delay>` (`messageExtras.Delayed`) is dropped as stale when it is a chat state or comes from groupchat (MUC join history); delayed DM markers and receipts (offline storage) are processed, and the DM rules are skipped for a bare JID that is a room joined this session (`isJoinedRoomBare`: MUC private messages, also as carbons); live 1:1 body-less chat state -> `simplevent.Typing` (carbons, MUC, group MMS ignored); `<displayed>` -> `RemoteEventReadReceipt` (live DM, carbon sent = IsFromMe, carbon received, MUC own occupant = IsFromMe, MUC other occupant only when `mucOccupantSeen` has a ghost); `<received>` -> `RemoteEventDeliveryReceipt` (live 1:1 only). `newMarkerReceipt`: the pre-handler ignores a read receipt "from me" when the user has no double puppet (`ownReceiptGate`, else bridgev2 would use the user's own ghost and fail with 403), then resolves the raw ref room-scoped (`findMessagesByXMPPRef`, `refReplyDM`/`refReplyMUC`) and requires the target to be ours (contact receipts) or not ours (our own markers). The body-less MUC path runs the echo check first, so our own marker's reflection (id registered in `mucEcho`) is dropped. A live 1:1 `<request/>` is answered from `jabberRemoteMessage.PostHandle` (`answerReceiptRequest`) only when the row exists in the portal, never for groupchat, carbons, MAM, our own account or group MMS. Outbound: `HandleMatrixTyping` (DM, not Cheogram, known peer), `HandleMatrixReadReceipt` (`pickDisplayedTarget`: newest foreign `Markable` row; MUC needs the room stanza-id and an already joined room, never joins; no markers to Cheogram JIDs; groupchat markers are trailing-debounced per portal by `mucMarkerDebounce` (8 s, newest ref only, one timer per portal in `signalState.mucPending`, stopped by `reset`/`Disconnect`, the timer goroutine `sendMUCDisplayed` starts no span) and carry no `<store/>`, DM markers do; a sent message with `<active/>` clears the typing record, `noteOutboundSent`), `withSignalFlags` (applied in `sendOut` and `handleMatrixMessage`: `<markable/>` always, `<request/>` and `<active/>` in chat type, `<active/>` only with `send_typing` and not for Cheogram). Seams: `signalHook`, `bridgedHook`, `betweenHook`, `doublePuppetHook`. Capabilities `ReadReceipts`/`TypingNotifications` are true (cap ID `...2026_11b`, capabilities version 4); `bridgeFeatureVars` lists chatstates, chat-markers and receipts.

### Subjects and encrypted messages

- A live MUC `<subject>` (no body) is bridged as the topic only when the bare JID is a room joined this session (`isJoinedRoomBare`, memory only), so contacts cannot change DM portals or create portals. `getMUCChatInfo` returns `Topic: nil` once the portal has a Matrix room (the live subject owns the topic; the placeholder/disco topic is for creation only), otherwise every rejoin resync would flip the topic.
- Encrypted messages (OMEMO, OpenPGP, `aesgcm://`) are bridged as a notice (reply context kept) and still get XEP-0184 receipts/markers: intentional, the bridge did deliver the notice. A plain message whose body starts with `aesgcm://` is treated as encrypted (accepted false positive).

### Protocol Extensions

- **`pkg/connector/xmpp_reply_ref.go`** — Reply/reaction target resolution (XEP-0461/0444): `resolveMessageRef` is room-scoped (`findMessagesByXMPPRef` with `refReplyMUC`: stanza_id, id, origin_id/xmpp_message_id; `refReplyDM`: id, origin_id/xmpp_message_id, stanza_id). MUC vs DM comes from `refIsMUC` (event origin, stored RoomType, JID heuristic; never disco). XEP-0428 reply fallback range (`replyFallback`, code points) is stripped only when the target resolves. Outbound ids: `replyRefForMessage` (MUC: target's room stanza-id, else legacy id).

- **`pkg/connector/xmpp_carbons.go`** — XEP-0280 Message Carbons. Enables on connect, parses `<sent>`/`<received>` forwarded stanzas, queues as remote events.

- **`pkg/connector/muc_autorejoin.go`** — MUC auto-rejoin: `Connect` starts `runMUCAutoRejoin(sessCtx)` (own root span, ends with the session context, 3 s initial delay, 500 ms stagger, max 200 rooms). `selectMUCRejoinCandidates` (pure) takes this login's `GetAllWithMXID` portals with a Matrix room that are MUCs (RoomType GroupDM; unset type -> `isMUC`; never DMs, Cheogram DM/group MMS, or `PortalMetadata.MUCLeft`). Joins go through `joinMUC(ctx, room, hist)`; the batch passes `mucRejoinHistory` (`<history since=newest bridged message, maxstanzas=100>`, since clamped to 7 days; `maxstanzas=20` without anchor), manual `JoinMUC` and 409 retries send no history element. `LeaveMUC` success sets `MUCLeft` (`setMUCLeft`), the join COMMAND (`JoinMUC`) clears it; sending, viewing and retracting use `joinMUCForPortal`, which never clears it (a left room returns `errMUCLeft`: the view path skips silently, sending fails with a notice) and never writes a bookmark. `runMUCAutoRejoin` is also the startup coordinator for bookmarks: bookmark gate + fetch first, then the portals minus those whose bookmark has autojoin=false (only when the fetch succeeded: marked left + notice), then the imports (`selectBookmarkImports`), one 500 ms stagger loop, then the one-time backfill.

- **`pkg/connector/muc_join_health.go`** — MUC join health (per session, memory only; `Connect` starts `runMUCJoinHealth(sessCtx)`, which ends with the context or a new `mucJoinGen`). Part 1: `parsePresence` fills `presenceInfo.errType`/`errCondition` (never the `<text>`); a non-409 presence error goes to `onMUCJoinError` (Serve, state only; acts only for a pending join with a matching nick): one `Warn "MUC join failed"` (condition/type only), permanent conditions (`mucJoinErrorPermanent`) drop the want entry and post one notice (`reportMUCJoinRefused`), transient ones make the watchdog retry. Part 2: `xmppClient.mucJoinWant` (guarded by `mucPendingJoinMu`, cleared by `resetMUCJoinState`) is set by `noteMUCJoinSent` (in `joinMUCNick` just before the send, so a fast answer finds it; a new entry is dropped again when the send fails; `mucJoinInFlight` also counts it) and dropped by confirmation (`noteMUCJoinConfirmedLocked`), `leaveMUC`, all-nicks-taken and permanent errors; each tick (`mucWatchdogTick`) `dueMUCJoinRetries` re-sends overdue joins (`mucJoinConfirmTimeout` + `mucJoinRetryDelay`, backoff to `mucJoinRetryMax`) via `joinMUCNick` with `mucRejoinHistory` under a root span `xmpp.muc.join_watchdog`, skipping (and dropping) confirmed or `MUCLeft` rooms; one "still trying" notice per room at `mucJoinNoticeAfter` attempts. Part 3: XEP-0410 self-ping of every confirmed occupant JID (`mucSelfPingInterval` 15 min, first one an interval after first sight, max 5 per tick, `sendPing` with `routePing`, `pingFunc` seam): `classifySelfPing` (not-acceptable = not joined, success/service-unavailable/feature-not-implemented/item-not-found = joined per XEP-0410, anything else inconclusive; the inconclusive log carries the condition only, never `stanza.Error.Error()`, which is the server text); not joined forgets the occupant JID and re-joins with the held nick (span `xmpp.muc.self_ping_rejoin`). Successful pings leave no span or log.

- **XEP-0402 bookmarks and XEP-0115 caps** (untagged, all builds; plan and decisions in `.plans/muc-bookmarks.md`). The upstream `mellium.im/xmpp/bookmarks` codec is NOT used for anything written back (it drops `<extensions>` on token decode and emits duplicate xmlns); `bookmarkConf` keeps extensions as copied tokens. Files:
  - **`xep0402_bookmarks.go`** — pure core: `bookmarkConf` codec, `bookmarkKey` (lower-cased bare JID; all matching uses it, publishes reuse the stored item id), `mergeBookmark` (name/password/extensions of an existing item never touched; an existing non-empty nick is never overwritten; nick only written when the item has none), `selectBookmarkImports`, `parseBookmarkEvent`, `bookmarkEventTrusted` (spoofing guard: `from` must be our exact bare JID), `bookmarkSupportFromInfo` (read = identity pubsub/pep; write = plus `pubsub#publish-options`; `#compat` only logged).
  - **`xep0402_state.go`** — `bookmarkState` (`xmppClient.bm`, reset per session in `Connect`): gate, cache (never the base of a write), `writeOff`, import budget (50 per session, shared by the connect import, live events and `bookmarks-sync`), event channel, test hooks (`discoHook`/`fetchHook`/`publishHook`). `ensureBookmarkGate`, `bookmarkStartup`, `bookmarkNick`/`bookmarkPassword` (memory only).
  - **`xep0402_write.go`** — `publishBookmarkFor`: gate -> cache pre-check -> FRESH `fetchBookmarks` -> `mergeBookmark` -> publish with publish-options (persist_items, max_items=max, send_last_published_item=never, access_model=whitelist). A refusal (conflict/feature-not-implemented/bad-request/forbidden/not-allowed) disables writes for the session and is never retried without options. Writers: the join command after the join is confirmed (`bookmarkAfterJoin`, from `handlePresence` via `mucJoinTrack.writeBookmark`; the nick is only offered when no 409 alternative was used) and `leave` (`bookmarkAfterLeave`: autojoin=false, bookmark kept, never created). Nothing else writes except the backfill.
  - **`xep0402_import.go`** — import helpers (`importBookmarkedRoom` = `joinMUC` with 20 stanzas of history + `ChatResync` with `CreatePortal` and `getImportedMUCChatInfo`), notices, `BookmarkSyncer`/`SyncBookmarks` (the `bookmarks-sync [push]` command).
  - **`xep0402_backfill.go`** — one-time automatic backfill (D5): bookmarks (autojoin=true, nick = the nick the bridge currently uses) for bridged MUC portals without a bookmark; existing bookmarks are never modified; marker `Extra["bookmarks_backfill"]` (`setLoginExtraKey`); only when writes are supported; the marker is set only if nothing failed. `bookmarks-sync push` re-runs it.
  - **`xep0402_events.go`** — live PEP events: `handlePubSubEvent` (Serve, memory only, non-blocking send) -> one worker per session (`runBookmarkEventWorker`). Joins act on an autojoin TRANSITION to on (not on edits), leaves on autojoin off or retract while in the room (leave MUC, `MUCLeft`, portal kept, notice); cache compare ignores our own echoes; the bridge never writes in reaction to an event.
  - **`xep0402_probe.go`** — `capsDiscoHandler` (in-memory disco#info answer for node "" and `<caps node>#<ver>`), `handlePubSubEvent`, `fetchBookmarks`. **`xep0115_caps.go`** — `bridgeFeatureVars` (exactly what the bridge consumes inbound; single source for hash and reply), `<c/>` on the initial presence only (never on MUC joins).
  - Nick precedence for a join (`chooseMUCNick`): explicit `muc-nick` > bookmark nick > remembered 409 alternative (valid only while its base is unchanged; derived from the bookmark nick when that is the base) > `muc-nick-default` > login localpart. The bookmark password is added to every join presence (`mucJoinTokens`, also 409 retries); never logged.
  - Own nick changes (status 303 for our occupant, `noteOwnMUCNickChange`) update `mucOurOccupantJID`, which all echo matching reads. `mucOurOccupantJID` is per session (cleared by `resetMUCJoinState`). `joinMUCForPortal` (send/view/retract) never clears `MUCLeft` (`errMUCLeft`) and, when already in the room, re-sends the join presence with the nick currently held (never renames a shared nick back); only the join command re-applies the nick choice.
  - Fetch safety: `fetchBookmarks` reports undecodable items and truncation (`bookmarkFetchInfo.mayHide`); writes and the backfill never create an item that may exist but was not read. The cache is set before each publish (so its PEP echo compares equal) and restored on failure.

- **`pkg/connector/xmpp_mam.go`** — XEP-0313 Message Archive Management. Backfill-on-view (`FetchMessages`), periodic catch-up loop (~45s interval), deduplication against recent messages. Catch-up and full backfill skip Cheogram number JIDs (`cheogramE164`, in `cheogram.go`) that have no Matrix room and were not seen on this connection (`mamEligible`, `roomPortalIDs`: one `GetAllWithMXID` query per cycle), so bulk-imported numbers do not cause polling or portal creation.

- **`pkg/connector/xep0363.go`** — XEP-0363 HTTP File Upload. Discovers upload service via common subdomains, requests upload slots, uploads via HTTP PUT.

- **`pkg/connector/xep0054.go`** — XEP-0054 vCard avatars for 1:1 contacts (`fetchVCardPhoto`, 1 MiB cap + `image/*` sniff via `validateAvatarBytes`, shared `applyGhostAvatar`). **`xep0054_muc.go`** / **`muc_presence.go`** — MUC occupant avatars: `parsePresence` (one token pass: XEP-0153 hash tri-state, item jid, status codes), in-memory occupant table (`noteMUCPresence`/`noteMUCGhost`, Serve goroutine, memory + non-blocking enqueue only), one worker per session (`startMUCAvatars`, paced occupant-JID vcard-temp get with real-JID fallback, `decideMUCAvatar`, MXC cache). When the 1:1 path applies a new avatar or removal for a bare JID (`doAvatarRefresh`), it calls `propagateContactAvatar` (memory only, any goroutine): occupants of this session whose room-reported `item jid` matches (case-insensitive) and that have a ghost get a no-IQ job applying the cached MXC (bypasses the 60 s interval; marks the advertised state handled so stale room presence cannot revert it). Contact rule (`applyContact`): when the room reports a real JID and the 1:1 contact ghost for that bare JID has a known avatar (`classifyContactGhost`: proven MXC, or fetched and known-none; read as a fresh DB row via `loadContactAvatar`, never the shared cached ghost), the contact avatar is authoritative for the occupant on every worker job (presence, first message, or an unknown-photo presence once per session): its MXC is applied or the avatar removed, no IQ, room-hash fetch skipped; otherwise the room-hash path runs. A room advertising a different state than the contact triggers `queueAvatarRefresh` for the contact (`maybeRefreshContact`: contact fetched over 10 min ago, max one per contact per 10 min and one per advertised state per 6 h; result flows back only via propagation). Occupants are only fetched after a ghost was seen this session (they spoke); the worker is the sole writer of MUC occupant avatars, so `getUserInfo` must keep returning a nil Avatar for them. No config toggle.

- **`pkg/connector/cheogram.go`** — Cheogram/JMP.chat integration. Phone number normalization (+1 default), short code handling, group MMS JID construction (comma-separated sorted numbers), sender extraction from body prefix.

### CardDAV contact import (untagged, all builds)

Phone numbers from a CardDAV address book are added to the roster as Cheogram contacts (`network.cheogram` required). Add-only, manual, read-only towards the server. Files in `pkg/connector/`:

- **`carddav_config.go`** — `CardDAVConfig` (`network.carddav`: `allow_insecure_http`, `allow_private_hosts`, `default_country_code`), example config, upgrader copies; the limits are constants (`cardDAVMaxContacts` 5000, `cardDAVMaxAddsPerRun` 500, `cardDAVAddsPerSecond` 5, `cardDAVMaxResponseBytes` 32 MiB).
- **`carddav_creds.go`** — `Extra["carddav"]` = `{v,url,username,password,book,books}` (plaintext password), saved through `setLoginExtraKey`.
- **`carddav_http.go`** — HTTP client seam `newCardDAVHTTPClient` (no proxy, no redirects followed, SSRF dialer `blockedCardDAVAddr`, TLS 1.2+), `normalizeCardDAVURL`, `sameSite` (eTLD+1 rule for credentials/redirects/hrefs), classified error kinds (`cardDAVError`; spans/logs record only the kind).
- **`carddav_dav.go`** — hand-rolled PROPFIND/REPORT with streaming, per-`<response>` tolerant decoding (bad entries counted, 507 = truncated flag, size cap errors instead of truncating).
- **`carddav_client.go`** — discovery (explicit book, principal/home-set, manual `/.well-known/carddav` hop), book selection (skips `z-app-generated*` and `collected`), `addressbook-query` with an `addressbook-multiget` fallback, per-book error isolation (`fetchAll`), `fetchContacts` (span `carddav.fetch`), `validateCardDAVLogin`.
- **`carddav_phone.go`**, **`carddav_cards.go`**, **`carddav_plan.go`** — pure core: TEL normalization (pre-pass + `mrtc-sip-bridge/pkg/phone`), vCard extraction (name chain, labels incl. sanitized custom `X-ABLabel`, vCard 2.1 salvage), planner (`planCardDAVSync`: dedupe, `Name (label)`, already-in-roster skip).
- **`carddav_sync.go`** — executor `applyPlan` (rate limit, 5-consecutive-failure abort, disconnect stop, `inRoster` re-check), per-login busy map and in-memory last-run summary, `cardDAVSyncJob` (detached; replies through a copied `commands.Event`, never the handler's).
- **`carddav_commands.go`**, **`carddav_prompt.go`** — `carddav-login|logout|status|sync` handlers and a local copy of `hostkit.PromptFields` (hostkit is tagged-only); **`carddav_roster.go`** — `xmppClient.rosterSnapshot()`.
- Shared untagged helpers used by both CardDAV and the sipcalls files: `metadata.go` (`setExtraKey`, `loginMeta`, `saveUserLogin`, `extraWriteMu`, `setLoginExtraKey`), `commands_login.go` (`lookupCommandLogin`, `checkJabberLogin`). All writers of `JabberLoginMeta.Extra` must take `extraWriteMu` (via `setLoginExtraKey` or directly); lock order `extraWriteMu` before `callsExt.mu`, never held across network I/O.
- Tests use a fake TLS CardDAV server (`carddav_fake_test.go`, generic and SOGo layouts). `TestCardDAVSmoke` runs against a real server when `CARDDAV_SMOKE_URL`, `CARDDAV_SMOKE_USER` and `CARDDAV_SMOKE_PASS` are set (e.g. a local Radicale container); otherwise it skips.

### Log scrubbing and preflight (`cmd/jmpchat-matrix-bridge/`)

- **`logscrub.go`** — `scrubLogging` wraps every configured zeroconfig writer (installed in `main` between `PreInit` and `Init`, i.e. `Run` expanded); `scrubLine` replaces `req_body` with `"<content omitted>"` for request URLs containing `/send/`, `/redact/` or `/state/`. No off switch (issue #12). Adapted from mautrix-xmpp (MIT, `LICENSE-THIRD-PARTY`). **`preflight.go`** — clear error (exit 10) for a missing/unreadable config; appends `--no-update` when the config is read-only (not for `-g`); skipped for `-h/-v/-e/-c -`, so the Docker first-run example print is unaffected.

### Telemetry

- **`pkg/telemetry/`** — Optional OpenTelemetry tracing (`telemetry.go`: `Init`/`Shutdown`/env resolution; `span.go`: span and log-correlation helpers; `http.go`: HTTP transport wrapping; `env.go`: injectable getenv). Off unless an OTLP endpoint env var is set; when off nothing is installed and helpers are no-ops that return the input ctx unchanged. `Init` never fails the bridge (problems log one warning and leave tracing disabled). Conventions: goroutines never receive a live span (start their own span, or a linked root); leaf operations use child-only spans so uninstrumented paths do not create orphan roots; never record message bodies, captions, emoji, filenames or URLs (full JIDs are allowed); never block or do IQ round trips on the mellium Serve goroutine, and close IQ responses/iterators (e.g. `roster.Fetch`, bookmark/pubsub fetches) before doing slow work: Serve hands each IQ result to its waiter and reads nothing else until that response is closed (an open roster iterator during the ghost refresh once stalled every IQ for up to a minute at connect). Log lines follow the same rule for message content (issue #12: log lengths/counts/ids, never bodies, captions, reaction emoji, filenames or media/upload URLs (HTTP client errors go through `redactURLError`, since `*url.Error` prints the URL); `log_privacy_test.go` feeds sentinel content through the inbound handlers); bookmark code never logs a bookmark's name, password or extensions. Log correlation: root spans and `bridge.handle_remote_event` annotate the ctx logger (`zerolog.Ctx`) with `trace_id`/`span_id`/`dd.trace_id`/`dd.span_id`; our own lines under a span use `x.logFor(ctx)` (or `telemetry.Logger(ctx, cleanLogger)`), never `x.log` directly; never pass a logger derived from `zerolog.Ctx(ctx)` to `telemetry.Logger`.

### Data & Metadata

- **`pkg/connector/dbmeta.go`** — Database metadata types: `PortalMetadata` (last sync, MUC nick), `GhostMetadata` (profile fetch time), `MessageMetadata` (XMPP stanza ID, backfill source).

- **`pkg/connector/metadata.go`** — `JabberLoginMeta` (password, default MUC nick; `Server`/`Port`/`DirectTLS` are the per-login server override set by `login password-server` and reused by the plain re-login via `existingDialOptions`).

- **`pkg/connector/roster_contacts.go`** — Named-roster snapshot and pure helpers (`phoneContacts`, `pageContacts`) behind the Dialpad widget Contacts tab (named Cheogram phone numbers only; sort and search rules documented in the file).

- **`pkg/connector/id.go`** — `EventSender` construction from JIDs.

- **`pkg/connector/capabilities.go`** — Declared capabilities: text, image/video/audio/file (50 MiB), captions, replies, reactions.

- **`pkg/connector/stub_client.go`** — Minimal `NetworkAPI` for logins without credentials (allows list/logout only).

## Bot Commands

All commands use the configured prefix (default `!jabber`):

| Command | Description |
|---------|-------------|
| `login password` | Log in with JID + password |
| `login password-server <jid> <password> <server>` | Same, connecting to a specific server (`host`, `host:port`, `tls://host:port` for direct TLS, or `auto` to clear a stored override) |
| `pm <jid-or-phone>` | Start a DM (Cheogram: phone numbers resolved automatically) |
| `join [<login>] <room@conference.domain>` | Join a MUC room (also creates/enables its bookmark, auto-join on, and is the only way back into a room left via `leave` or a bookmark turned off elsewhere) |
| `leave [<login>] <room@conference.domain>` | Leave a MUC room (an existing bookmark is kept with auto-join off) |
| `sync-roster [<login>]` | Re-fetch roster, update display/room names |
| `add-to-roster [<login>] <jid> <name>` | Add or rename a roster contact (inside a one-to-one chat, `add-to-roster <name>` works like `save-contact`) |
| `save-contact [<login>] <name>` | Inside a one-to-one chat room: save (or rename) that chat's contact in the roster; the room name updates. Works in every build |
| `remove-from-roster [[<login>] <jid-or-phone>]` | Remove a contact from the roster; with no argument, inside a one-to-one chat, removes that chat's contact. Names fall back to the JID/number. Works in every build |
| `carddav-login` | Set up the CardDAV account (management room, no arguments; prompts for URL, username, password, optional address book; validates before saving; needs `network.cheogram`) |
| `carddav-sync` / `carddav-sync apply` | Preview / apply the import of CardDAV phone numbers into the roster (detached run, result as a later message) |
| `carddav-status` / `carddav-logout` | Show the CardDAV account and last sync / remove the account |
| `roster-only-calls on\|off\|status` | (`sipcalls` build) Ring for incoming SIP calls only from numbers in the roster; others get 480 plus a notice in the management room. Gates SIP calls only, not XEP-0353 call notifications |
| `bookmarks-sync [<login>] [push]` | Import the XEP-0402 bookmarks with auto-join that have no Matrix room yet (same per-session budget of 50; never leaves a room, never writes). `push` re-runs the one-time backfill that creates bookmarks for already-bridged rooms without one (existing bookmarks are never modified) |
| `search-roster [<login>] <query>` | Search roster by JID or name |
| `muc-nick [<login>] <room-jid> <nick>` | Set room-specific MUC nickname (wins over the bookmark nick, which is left unchanged) |
| `muc-nick-default [<login>] <nick>` | Set default MUC nickname for all rooms |

Account selection: with several Jabber logins, the commands above taking `[<login>]` accept the login ID (lower-cased JID) as an optional first argument (`CommandLogin` in `pkg/connector/command_login.go`, same convention as bridgev2 `getClientForStartingChat`). Only the sender's own logins match, case-insensitively; the argument is not consumed if it would leave fewer arguments than the command needs (so `join me2@x.org` still means the room and `remove-from-roster me2@x.org` the contact; `add-to-roster` takes a login only before a JID, so `add-to-roster me2@x.org Full Name` is the contact; to add your own other account to the roster write `add-to-roster me1@x.org me2@x.org Name`). Without it: the chat room's login inside a portal, else the default login (first by ID); outside a portal with several logins the success reply notes which account was used. carddav/sipcalls commands use `lookupCommandLogin` only (no explicit argument).

## Setup for Development / Testing

See `README.md` for full setup instructions including Docker. Quick summary:

```bash
# Generate example config (writes config.yaml directly; use -c - to print to stdout)
./jmpchat-matrix-bridge -e

# Edit config.yaml (set homeserver.address, homeserver.domain, database, appservice, etc.)

# Generate appservice registration (give to Synapse)
./jmpchat-matrix-bridge -g

# Run the bridge
./jmpchat-matrix-bridge
```

Default appservice port is **29318**.

## Docker

A `Dockerfile` and `entrypoint.sh` are provided. The entrypoint prints an example config to stdout when no config is mounted, making first-run setup easy:

```bash
docker build $(./build-args.sh) -t williamkray/jmpchat-matrix-bridge .   # build-args.sh passes git commit/tag/repo/build time
docker run --rm williamkray/jmpchat-matrix-bridge > config.yaml   # get example config
```

See `README.md` for full Docker usage with registration generation and volume mounts.

## Key Dependencies

- Go 1.26+ (per `go.mod`)
- `maunium.net/go/mautrix v0.32.0` — bridgev2 framework (Matrix protocol, appservice, DB schema)
- `go.mau.fi/util v0.11.0` — shared utilities
- `mellium.im/xmpp v0.23.0` — XMPP client library
- `mellium.im/sasl v0.3.2` — SASL authentication
- `mellium.im/xmlstream v0.15.4` — XML stream processing
- Database: PostgreSQL 10+ (`github.com/lib/pq`) or SQLite (`github.com/mattn/go-sqlite3`)
- Logging: `github.com/rs/zerolog`

## Configuration

`config.yaml` is generated by the binary. The Jabber-specific section (`network:`) maps to `JabberConfig` in `connector.go`:

- `resource_prefix` — XMPP resource prefix
- `cheogram` — Enable Cheogram phone number resolution (also via `JMPCHAT_BRIDGE_CHEOGRAM=1` env var; the pre-rename `MAUTRIX_JABBER_CHEOGRAM` is still accepted)

The bridge is not bound to any single XMPP server: the connection host is derived from the JID's domain at login (`dialXMPP` does SRV lookup for `_xmpps-client`/`_xmpp-client`, falling back to the domain on port 5222; a per-login override is set with `login password-server`). The only server-related config key is `network.allow_private_server_override` (default false). Chat signal keys (all default true): `network.send_typing`, `network.send_read_markers`, `network.send_delivery_receipts`, `network.muc_read_markers` (see `xmpp_signals.go`; new keys must be present uncommented in the example config and copied in the upgrader).

Tracing is configured only through environment variables (no config-file keys), see `pkg/telemetry` and the README "Tracing (OpenTelemetry)" section. It is enabled iff `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set. Protocol comes from `OTEL_EXPORTER_OTLP_(TRACES_)PROTOCOL` and defaults to `http/protobuf` per the OTel spec (the code does not infer it from the port; always set it explicitly: `grpc` for :4317, `http/protobuf` for :4318). `OTEL_SDK_DISABLED=true` or `OTEL_TRACES_EXPORTER=none` disables it. `DD_SERVICE`/`DD_VERSION`/`DD_ENV` are fallbacks for resource attributes; `OTEL_SERVICE_NAME`/`OTEL_RESOURCE_ATTRIBUTES` override them.

CardDAV import is configured under `network.carddav` (`allow_insecure_http`, `allow_private_hosts`, `default_country_code`); see the CardDAV section in `README.md`.

Other relevant framework config sections: `bridge.permissions`, `matrix.delivery_receipts`, `backfill.enabled` / `backfill.max_initial_messages`, `encryption.*`.

`registration.yaml` is auto-generated and must be registered with the Matrix homeserver.

### Test Infrastructure

For local Docker testing, `docker-run/` (gitignored) contains a self-contained `config.yaml` + `registration.yaml` pair with a local SQLite database. `docker-registration.yaml` (gitignored) holds the matching registration used when generating the docker-run setup.

A local verification config also exists: `config.verify.yaml` / `registration.verify.yaml` (gitignored) with a SQLite database (`mautrix-jabber-verify.db`) for quick startup testing without a homeserver.

## Related Documents

- **[README.md](README.md#to-do-unimplemented-work)** — Canonical to-do list of pending work
- **[ROADMAP.md](ROADMAP.md)** — Phased production plan, implementation notes, known issues
- **[CODE_OPTIMIZATION.md](CODE_OPTIMIZATION.md)** — Code optimization backlog (duplication, config, structure)

**Encryption is deprioritized** — do not implement OMEMO or add XMPP crypto dependencies until core features are stable.

## SIP calls

Calls for Cheogram/JMP numbers (SIP -> MatrixRTC) are behind build tag `sipcalls`, in files named `pkg/connector/*sipcalls*.go` (plus `pkg/telemetry/backend_sipcalls*.go` and `cmd/jmpchat-matrix-bridge/variant_sipcalls.go`); untagged files only hold call-free extension seams (`extension.go`). `sipcalls_widgetfeatures.go` implements the Dialpad widget's optional host features (`hostkit.RosterOnlyPrefs`/`ChatOpener`/`ContactSaver`/`ContactLookup`, wired as `Deps.Features`): roster-only toggle, Message (reuses `ResolveCallRoom`, detached from the request, MAM backfill afterwards via `xmppClient.backfillChatAsync`), Save contact (the `save-contact` roster set) and the saved-name lookup; the widget and this file never log or trace contact names (the roster-fetch log line in `syncRoster` still logs roster names; `SetRosterItem` and the roster-push handler no longer do). The tag is included in the default Docker build/image (with `nolibopusfile`); calls stay off unless `network.calls.enabled: true`. The call core is the public module `github.com/williamkray/mrtc-sip-bridge`; the host contract and user docs are in its `docs/EMBEDDING.md`. Local dev builds with calls use `-tags "nocrypto sipcalls nolibopusfile"` (needs libopus dev headers and pkg-config).
