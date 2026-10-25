package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/connector"
	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// Filled at build time with -X linker flags (e.g. -X main.Tag=... -X main.Commit=... -X main.BuildTime=...)
var (
	Tag           = "unknown"
	Commit        = "unknown"
	BuildTime     = "unknown"
	RepositoryURL = "unknown"
)

func cmdPrefix(ce *commands.Event) string {
	if ce.Bridge.Config != nil && ce.Bridge.Config.CommandPrefix != "" {
		return ce.Bridge.Config.CommandPrefix
	}
	return "!jabber"
}

// withSpan wraps a command handler in a root span with a static name. cmd.name is the only
// attribute; command arguments are never recorded. ce.Ctx is replaced for the handler's duration.
func withSpan(name string, fn func(*commands.Event)) func(*commands.Event) {
	return func(ce *commands.Event) {
		orig := ce.Ctx
		base := orig
		if base == nil {
			base = context.Background()
		}
		ctx, span := telemetry.Start(base, "cmd."+strings.ReplaceAll(name, "-", "_"),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attribute.String("cmd.name", name)))
		defer span.End()
		ce.Ctx = ctx
		defer func() { ce.Ctx = orig }()
		if ce.Log != nil {
			origLog := ce.Log
			ce.Log = telemetry.Logger(ctx, ce.Log)
			defer func() { ce.Log = origLog }()
		}
		fn(ce)
	}
}

// knownOrEmpty maps the "unknown" linker-flag default to "".
func knownOrEmpty(s string) string {
	if s == "unknown" {
		return ""
	}
	return s
}

func main() {
	br := &mxmain.BridgeMain{
		Name:        "jmpchat-matrix-bridge",
		Description: "A Matrix-Jabber (XMPP) puppeting bridge, focused on JMP.chat/Cheogram (SMS/MMS, SIP calls, CardDAV contacts).",
		URL:         "https://github.com/williamkray/jmpchat-matrix-bridge",
		// DBOwner pins the database owner to the value the bridge had when it was called
		// mautrix-jabber. mxmain derives the owner from Name ("megabridge/"+Name) and the
		// database layer refuses to start if the stored owner differs, so renaming without
		// this would lock every existing deployment out of its database. Never change it.
		DBOwner:   "megabridge/mautrix-jabber",
		Version:   "0.12.0" + buildVariant,
		Connector: &connector.JabberConnector{},
	}
	br.PostInit = func() {
		telemetry.Init(context.Background(), telemetry.Options{
			ServiceVersion: br.Version,
			CommitSHA:      knownOrEmpty(Commit),
			RepositoryURL:  knownOrEmpty(RepositoryURL),
			Log:            *br.Log,
		})
		if telemetry.Enabled() {
			hc := br.Matrix.AS.HTTPClient
			hc.Transport = telemetry.WrapMatrixTransport(hc.Transport)
		}
		// Inject decoded network config into connector (bridge only receives BridgeConfig, not Network).
		var netCfg connector.JabberConfig
		if err := br.Config.Network.Decode(&netCfg); err == nil {
			if jc, ok := br.Connector.(*connector.JabberConnector); ok {
				jc.SetNetworkConfig(&netCfg)
				// Respect matrix.delivery_receipts for read receipts when bridging messages.
				jc.SetDeliveryReceipts(br.Config.Matrix.DeliveryReceipts)
				// Backfill config for MAM backfill-on-view and full backfill on login/sync-roster (config.backfill).
				jc.SetBackfillConfig(&connector.BackfillConfig{
					Enabled:            br.Config.Backfill.Enabled,
					MaxInitialMessages: br.Config.Backfill.MaxInitialMessages,
				})
				jc.SetAppServiceID(br.Config.AppService.ID)
			}
		}
		if jc, ok := br.Connector.(*connector.JabberConnector); ok {
			jc.PostInitExtensions(br.Bridge.Commands.(*commands.Processor), &br.Config.Network)
		}
		prefix := br.Config.Bridge.CommandPrefix
		if prefix == "" {
			prefix = "!jabber"
		}
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("sync-roster", cmdSyncRoster),
			Name:          "sync-roster",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Re-fetch roster from server and update Matrix display/room names; when backfill is enabled, also runs full MAM backfill for roster DMs. Usage: `%s sync-roster [_login JID_]`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("add-to-roster", cmdAddToRoster),
			Name:          "add-to-roster",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Add or rename a contact in your Jabber roster. Usage: `%s add-to-roster [_login JID_] <jid> <name>`, or `%s add-to-roster <name>` inside a one-to-one chat", prefix, prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("save-contact", connector.SaveContactCommand),
			Name:          "save-contact",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Save the person you are chatting with to your Jabber roster (run inside a one-to-one chat). Usage: `%s save-contact [_login JID_] <name>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("remove-from-roster", connector.RemoveContactCommand),
			Name:          "remove-from-roster",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Remove a contact from your Jabber roster. Usage: `%s remove-from-roster [_login JID_] <jid-or-phone>`, or `%s remove-from-roster` inside a one-to-one chat", prefix, prefix)},
		})
		for _, c := range []struct {
			name, desc string
			fn         func(*commands.Event)
		}{
			{"carddav-login", fmt.Sprintf("Set up (or replace) the CardDAV address book to import phone numbers from; the bridge asks for each value. Needs `network.cheogram`. Usage: `%s carddav-login`", prefix), connector.CardDAVLoginCommand},
			{"carddav-logout", fmt.Sprintf("Remove the CardDAV account of this login (contacts already added to the roster stay). Usage: `%s carddav-logout`", prefix), connector.CardDAVLogoutCommand},
			{"carddav-status", fmt.Sprintf("Show the CardDAV account and the last sync. Usage: `%s carddav-status`", prefix), connector.CardDAVStatusCommand},
			{"carddav-sync", fmt.Sprintf("Import phone numbers from your CardDAV address book into your Jabber roster as Cheogram contacts. Without an argument it only previews. Usage: `%s carddav-sync` or `%s carddav-sync apply`", prefix, prefix), connector.CardDAVSyncCommand},
		} {
			br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
				Func:          withSpan(c.name, c.fn),
				Name:          c.name,
				RequiresLogin: true,
				Help:          commands.HelpMeta{Section: commands.HelpSectionAuth, Description: c.desc},
			})
		}
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("search-roster", cmdSearchRoster),
			Name:          "search-roster",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Search your Jabber roster for contacts matching a JID or name. Usage: `%s search-roster [_login JID_] <query>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("join", cmdMUCJoin),
			Name:          "join",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Join a multi-user chat (MUC) room. Usage: `%s join [_login JID_] <room-jid>` (e.g. room@conference.example.com)", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("leave", cmdMUCLeave),
			Name:          "leave",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Leave a multi-user chat (MUC) room. Usage: `%s leave [_login JID_] <room-jid>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("bookmarks-sync", cmdBookmarksSync),
			Name:          "bookmarks-sync",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Import the rooms of your XMPP bookmarks (auto-join on) that have no Matrix room yet; never leaves a room or changes a bookmark. `push` also creates bookmarks for the rooms this bridge is already in that have none (existing bookmarks are never modified). Usage: `%s bookmarks-sync [_login JID_] [push]`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("muc-nick", cmdMUCNick),
			Name:          "muc-nick",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Set preferred MUC nickname for a room. Usage: `%s muc-nick [_login JID_] <room-jid> <nick>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("avatar-probe", connector.CmdAvatarProbe),
			Name:          "avatar-probe",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Diagnostic: ask a JID for its avatar (vcard-temp, XEP-0084 metadata/data, disco#info) and report what came back. Sends only IQ get queries, never messages or presence. Usage: `%s avatar-probe [_login JID_] <room@service/nick | jid>`", prefix)},
		})
		br.Bridge.Commands.(*commands.Processor).AddHandler(&commands.FullHandler{
			Func:          withSpan("muc-nick-default", cmdMUCNickDefault),
			Name:          "muc-nick-default",
			RequiresLogin: true,
			Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: fmt.Sprintf("Set default MUC nickname for all rooms. Usage: `%s muc-nick-default [_login JID_] <nick>`", prefix)},
		})
	}
	br.PostStart = func() {
		attrs := []attribute.KeyValue{
			attribute.String("bridge.version", br.Version),
			attribute.Bool("backfill.enabled", br.Config.Backfill.Enabled),
			attribute.Bool("cheogram.enabled", connector.CheogramEnabled(br.Bridge)),
		}
		if jc, ok := br.Connector.(*connector.JabberConnector); ok {
			attrs = append(attrs, jc.ExtensionStartAttrs()...)
		}
		_, span := telemetry.Start(context.Background(), "bridge.start", trace.WithAttributes(attrs...))
		span.End()
	}
	br.InitVersion(Tag, Commit, BuildTime)
	// Same as br.Run(), with the log scrubber installed after the config is loaded and before
	// mxmain compiles the logger and creates the Matrix client.
	preflight()
	br.PreInit()
	scrubLogging(&br.Config.Logging)
	br.Init()
	br.Start()
	exitCode := br.WaitForInterrupt()
	br.Stop()
	os.Exit(exitCode)
}

func cmdSyncRoster(ce *commands.Event) {
	login, _, explicit := connector.CommandLogin(ce, 0)
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	rs, ok := login.Client.(connector.RosterSyncer)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	// Roster sync now fetches a vCard avatar per contact, so it can take a while. Reply
	// immediately and run the sync (and optional full backfill) in the background so the
	// "started" message isn't held until the whole sync completes. Detach from ce.Ctx so the
	// work isn't cancelled when the command handler returns.
	bf, hasBackfill := login.Client.(connector.FullBackfillRunner)
	notice := connector.CommandLoginNotice(ce, login, explicit)
	if hasBackfill {
		ce.Reply("Roster sync started. Display names and room names will update shortly. Full backfill (if enabled) is running in the background.%s", notice)
	} else {
		ce.Reply("Roster sync started. Display names and room names will update shortly.%s", notice)
	}
	ctx := context.WithoutCancel(ce.Ctx)
	// Carry only the span context so the goroutine never touches the handler's live span.
	from := telemetry.CarrySpan(context.Background(), ce.Ctx)
	go func() {
		ctx, span := telemetry.StartLinkedRoot(ctx, "cmd.sync_roster.background", from)
		defer span.End()
		rs.SyncRoster(ctx)
		if hasBackfill {
			// Full MAM backfill for all roster DMs so portals get history without needing to log
			// out and back in (or wait for full_backfill_on_login on next connect).
			bf.RunFullBackfill(ctx)
		}
	}()
}

func cmdAddToRoster(ce *commands.Event) {
	// Inside a chat, `add-to-roster <name>` (no JID) saves the chat's contact, like save-contact.
	// An own login ID as the first argument selects the account only in `add-to-roster <login> <jid> <name>`:
	// a JID followed by a (possibly multi-word) name is always the contact, even if it is one of the user's logins.
	login, args, explicit := connector.CommandLogin(ce, 2)
	if explicit && !strings.Contains(args[0], "@") {
		login, args, explicit = connector.CommandLogin(ce, len(ce.Args)) // never consumes
	}
	if ce.Portal != nil && len(args) > 0 && !strings.Contains(args[0], "@") {
		connector.SaveContactCommand(ce)
		return
	}
	if len(args) < 2 {
		ce.Reply("Usage: `%s add-to-roster <jid> <name>` — e.g. `%s add-to-roster friend@jabber.org My Friend`. Inside a one-to-one chat, `%s add-to-roster <name>` saves that contact.", cmdPrefix(ce), cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	jid := strings.TrimSpace(strings.ToLower(args[0]))
	name := strings.TrimSpace(strings.Join(args[1:], " "))
	if jid == "" || !strings.Contains(jid, "@") {
		ce.Reply("Invalid JID. Use the form user@domain (e.g. user@jabber.org).")
		return
	}
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	rm, ok := login.Client.(connector.RosterManager)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	if err := rm.SetRosterItem(ce.Ctx, jid, name); err != nil {
		ce.Reply("Failed to set roster name: %v", err)
		return
	}
	ce.Reply("Roster updated: **%s** is now **%s**. Display and room names should update shortly.%s", jid, name, connector.CommandLoginNotice(ce, login, explicit))
}

func cmdSearchRoster(ce *commands.Event) {
	login, args, explicit := connector.CommandLogin(ce, 1)
	if len(args) == 0 {
		ce.Reply("Usage: `%s search-roster <query>` — e.g. `%s search-roster megan` or `%s search-roster @example.com`", cmdPrefix(ce), cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	query := strings.TrimSpace(strings.Join(args, " "))
	if query == "" {
		ce.Reply("Query cannot be empty.")
		return
	}
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	searcher, ok := login.Client.(connector.RosterSearcher)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	results, err := searcher.SearchRoster(ce.Ctx, query)
	if err != nil {
		ce.Reply("Failed to search roster: %v", err)
		return
	}
	if len(results) == 0 {
		ce.Reply("No contacts found matching **%s**.", query)
		return
	}
	var lines []string
	for _, contact := range results {
		if contact.Name != "" {
			lines = append(lines, fmt.Sprintf("- **%s** (%s)", contact.Name, contact.JID))
		} else {
			lines = append(lines, fmt.Sprintf("- %s", contact.JID))
		}
	}
	ce.Reply("Found %d contact(s) matching **%s**:\n%s%s", len(results), query, strings.Join(lines, "\n"), connector.CommandLoginNotice(ce, login, explicit))
}

func cmdMUCJoin(ce *commands.Event) {
	login, args, explicit := connector.CommandLogin(ce, 1)
	if len(args) == 0 {
		ce.Reply("Usage: `%s join <room-jid>` — e.g. `%s join myroom@conference.example.com`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	roomJID := strings.TrimSpace(strings.ToLower(args[0]))
	if roomJID == "" || !strings.Contains(roomJID, "@") {
		ce.Reply("Invalid room JID. Use the form room@conference.domain (e.g. myroom@conference.example.com).")
		return
	}
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	muc, ok := login.Client.(connector.MUCJoiner)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	if err := muc.JoinMUC(ce.Ctx, roomJID); err != nil {
		ce.Reply("Failed to join room: %v", err)
		return
	}
	// Get or create the Matrix portal so the user sees the room.
	portalKey := networkid.PortalKey{ID: networkid.PortalID(roomJID), Receiver: login.ID}
	portal, err := ce.Bridge.GetPortalByKey(ce.Ctx, portalKey)
	if err != nil {
		ce.Reply("Joined **%s** on Jabber, but could not open Matrix room: %v", roomJID, err)
		return
	}
	if portal.MXID == "" {
		// The user asked to join a MUC: never let type detection (disco/heuristic) turn this into a DM.
		portal.RoomType = database.RoomTypeGroupDM
		info, err := login.Client.GetChatInfo(ce.Ctx, portal)
		if err != nil || info == nil {
			ce.Reply("Joined **%s** on Jabber. Opening Matrix room failed: %v", roomJID, err)
			return
		}
		if err := portal.CreateMatrixRoom(ce.Ctx, login, info); err != nil {
			ce.Reply("Joined **%s** on Jabber, but creating Matrix room failed: %v", roomJID, err)
			return
		}
		portal.UpdateBridgeInfo(ce.Ctx)
		portal.UpdateCapabilities(ce.Ctx, login, true)
		userIntent := ce.User.DoublePuppet(ce.Ctx)
		if userIntent != nil {
			_ = userIntent.EnsureJoined(ce.Ctx, portal.MXID)
		}
	}
	ce.Reply("Joined **%s**. Room: %s%s", roomJID, portal.MXID.String(), connector.CommandLoginNotice(ce, login, explicit))
}

func cmdMUCLeave(ce *commands.Event) {
	login, args, explicit := connector.CommandLogin(ce, 1)
	if len(args) == 0 {
		ce.Reply("Usage: `%s leave <room-jid>` — e.g. `%s leave myroom@conference.example.com`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	roomJID := strings.TrimSpace(strings.ToLower(args[0]))
	if roomJID == "" || !strings.Contains(roomJID, "@") {
		ce.Reply("Invalid room JID. Use the form room@conference.domain (e.g. myroom@conference.example.com).")
		return
	}
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	muc, ok := login.Client.(connector.MUCJoiner)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	if err := muc.LeaveMUC(ce.Ctx, roomJID); err != nil {
		ce.Reply("Failed to leave room: %v", err)
		return
	}
	ce.Reply("Left **%s**. If you have a bookmark for it, it is kept with auto-join turned off (your other clients stop joining it too). You can leave the Matrix room from the room menu if desired.%s", roomJID, connector.CommandLoginNotice(ce, login, explicit))
}

func cmdBookmarksSync(ce *commands.Event) {
	login, args, explicit := connector.CommandLogin(ce, 0)
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	push := false
	for _, a := range args {
		if strings.EqualFold(a, "push") {
			push = true
		} else {
			ce.Reply("Usage: `%s bookmarks-sync [push]`", cmdPrefix(ce))
			return
		}
	}
	bs, ok := login.Client.(connector.BookmarkSyncer)
	if !ok {
		ce.Reply("Not connected to Jabber. Reconnect (or wait for auto-reconnect) and try again.")
		return
	}
	res, err := bs.SyncBookmarks(ce.Ctx, push)
	if err != nil {
		ce.Reply("Bookmark sync failed: %v%s", err, connector.CommandLoginNotice(ce, login, explicit))
		return
	}
	if !res.ReadSupported {
		ce.Reply("Your server does not support bookmarks (XEP-0402 over PEP); nothing to sync.%s", connector.CommandLoginNotice(ce, login, explicit))
		return
	}
	msg := fmt.Sprintf("Bookmarks: %d total, %d with auto-join. Imported %d new room(s); %d already bridged, %d skipped (not a room), %d over the per-session limit, %d failed. Writing bookmarks is %s%s.",
		res.Total, res.Autojoin, res.Imported, res.Existing, res.Invalid, res.Capped, res.Failed,
		map[bool]string{true: "supported", false: "not available"}[res.WriteSupported],
		map[bool]string{true: " (server mirrors legacy bookmarks)", false: ""}[res.CompatSeen])
	if push {
		if res.BackfillRan {
			msg += fmt.Sprintf(" Push: created %d bookmark(s), %d room(s) already had one (unchanged), %d failed.", res.Created, res.AlreadyHad, res.PushFailed)
		} else {
			msg += " Push: not run (the server does not allow private bookmark writes, or the bookmarks could not be read)."
		}
	}
	ce.Reply("%s%s", msg, connector.CommandLoginNotice(ce, login, explicit))
}

func cmdMUCNick(ce *commands.Event) {
	login, args, explicit := connector.CommandLogin(ce, 2)
	if len(args) < 2 {
		ce.Reply("Usage: `%s muc-nick <room-jid> <nick>` — e.g. `%s muc-nick myroom@conference.example.com MyNick`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	roomJID := strings.TrimSpace(strings.ToLower(args[0]))
	nick := strings.TrimSpace(args[1])
	if roomJID == "" || !strings.Contains(roomJID, "@") {
		ce.Reply("Invalid room JID. Use the form room@conference.domain.")
		return
	}
	if nick == "" {
		ce.Reply("Nick cannot be empty.")
		return
	}
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	if err := connector.SetMUCPreferredNickForRoom(ce.Ctx, ce.Bridge, login.ID, roomJID, nick); err != nil {
		ce.Reply("Failed to set MUC nick: %v", err)
		return
	}
	ce.Reply("Preferred nick for **%s** set to **%s**. It will be used on next join (and on 409 retry); it takes precedence over the room's bookmark nick, which is left unchanged.%s", roomJID, nick, connector.CommandLoginNotice(ce, login, explicit))
}

func cmdMUCNickDefault(ce *commands.Event) {
	login, args, explicit := connector.CommandLogin(ce, 1)
	if len(args) == 0 {
		ce.Reply("Usage: `%s muc-nick-default <nick>` — e.g. `%s muc-nick-default MyNick`", cmdPrefix(ce), cmdPrefix(ce))
		return
	}
	nick := strings.TrimSpace(strings.Join(args, " "))
	if nick == "" {
		ce.Reply("Nick cannot be empty.")
		return
	}
	if login == nil {
		ce.Reply("You must be logged in. Use `%s login` first.", cmdPrefix(ce))
		return
	}
	if err := connector.SetDefaultMUCNick(ce.Ctx, ce.Bridge, login, nick); err != nil {
		ce.Reply("Failed to set default MUC nick: %v", err)
		return
	}
	ce.Reply("Default MUC nick set to **%s**. It will be used for rooms that don't have a room-specific nick.%s", nick, connector.CommandLoginNotice(ce, login, explicit))
}
