package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// errCardDAVNeedCheogram is the refusal when network.cheogram is off.
const cardDAVNeedCheogramText = "CardDAV sync adds phone numbers as Cheogram contacts. Enable `network.cheogram` first."

var cardDAVLoginBusy = struct {
	sync.Mutex
	m map[networkid.UserLoginID]bool
}{m: map[networkid.UserLoginID]bool{}}

func cardDAVCmdPrefix(ce *commands.Event) string {
	if ce.Bridge != nil && ce.Bridge.Config != nil && ce.Bridge.Config.CommandPrefix != "" {
		return ce.Bridge.Config.CommandPrefix
	}
	return "!jabber"
}

// cardDAVGate runs the checks shared by all commands: Cheogram on, a real jabber login.
func cardDAVGate(ce *commands.Event, needCheogram bool) (*bridgev2.UserLogin, bool) {
	if needCheogram && !CheogramEnabled(ce.Bridge) {
		ce.Reply("%s", cardDAVNeedCheogramText)
		return nil, false
	}
	ul, refusal := checkJabberLogin(lookupCommandLogin(ce), cardDAVCmdPrefix(ce))
	if ul == nil {
		ce.Reply("%s", refusal)
		return nil, false
	}
	return ul, true
}

// maskCardDAVUser shows only the first and last character of the username.
func maskCardDAVUser(u string) string {
	r := []rune(u)
	if len(r) <= 2 {
		return "***"
	}
	return string(r[0]) + "***" + string(r[len(r)-1])
}

func cardDAVLoginFields(cfg CardDAVConfig) []bridgev2.LoginInputDataField {
	return []bridgev2.LoginInputDataField{
		{
			Type: bridgev2.LoginInputFieldTypeURL, ID: "url", Name: "CardDAV server URL",
			Description: "The https:// address of your CardDAV server, e.g. https://cloud.example.org/remote.php/dav (Nextcloud), " +
				"https://mail.example.org/SOGo/dav/ (SOGo) or the address book's full URL, e.g. https://dav.example.org/user/contacts/ (Radicale). " +
				"A bare host name works too.",
			Validate: func(s string) (string, error) {
				u, err := normalizeCardDAVURL(s, cfg)
				if err != nil {
					return "", err
				}
				return u.String(), nil
			},
		},
		{Type: bridgev2.LoginInputFieldTypeUsername, ID: "username", Name: "CardDAV username"},
		{
			Type: bridgev2.LoginInputFieldTypePassword, ID: "password", Name: "CardDAV password",
			Description: "Use an app-specific password if your provider offers one. The bridge only reads your contacts; it never changes your address book.",
		},
		{
			Type: bridgev2.LoginInputFieldTypeUsername, ID: "book", Name: "Address book", Optional: true,
			Description: "Name of the address book to sync, or skip for all of them.",
		},
	}
}

// CardDAVLoginCommand is the handler of `carddav-login`.
func CardDAVLoginCommand(ce *commands.Event) {
	prefix := cardDAVCmdPrefix(ce)
	if len(ce.Args) > 0 {
		ce.Redact() // the arguments may be a password
		ce.Reply("%s", fmt.Sprintf("Run `%s carddav-login` without arguments; the bridge asks for each value.", prefix))
		return
	}
	// Prompt answers are unprefixed: outside the management room bridgev2 would not treat them as commands
	// and the bridge would send them to the contact.
	if !ce.Sudo && ce.RoomID != ce.User.ManagementRoom {
		ce.Reply("%s", fmt.Sprintf("Run `%s carddav-login` in your management room (your DM with the bridge bot): answers typed in a chat room would be sent to the contact.", prefix))
		return
	}
	ul, ok := cardDAVGate(ce, true)
	if !ok {
		return
	}
	if loginMeta(ul) == nil {
		ce.Reply("Log in to Jabber first.")
		return
	}
	cfg := cardDAVConfigOf(ce.Bridge)
	ce.Reply("Enter your CardDAV account details. Answer each prompt as its own message (the password message is deleted). " +
		"The bridge only reads your address book and adds missing phone numbers to your Jabber roster. " +
		"The password is stored in plaintext in the bridge database, like your Jabber password.")
	cardDAVPromptFields(ce, "CardDAV login", cardDAVLoginFields(cfg),
		func(ce *commands.Event, input map[string]string) { submitCardDAVLogin(ce, ul, cfg, input) })
}

// submitCardDAVLogin connects and discovers before anything is saved.
func submitCardDAVLogin(ce *commands.Event, ul *bridgev2.UserLogin, cfg CardDAVConfig, input map[string]string) {
	prefix := cardDAVCmdPrefix(ce)
	cardDAVLoginBusy.Lock()
	busy := cardDAVLoginBusy.m[ul.ID]
	cardDAVLoginBusy.m[ul.ID] = true
	cardDAVLoginBusy.Unlock()
	if busy {
		ce.Reply("A CardDAV login is already in progress for this login.")
		return
	}
	defer func() {
		cardDAVLoginBusy.Lock()
		delete(cardDAVLoginBusy.m, ul.ID)
		cardDAVLoginBusy.Unlock()
	}()
	ctx, span := telemetry.Start(ce.Ctx, "cmd.carddav_login.submit", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	outcome := "error"
	defer func() { span.SetAttributes(attribute.String("carddav.outcome", outcome)) }()
	creds, books, err := validateCardDAVLogin(ctx, cfg, input["url"], input["username"], input["password"], input["book"])
	if err != nil {
		outcome = cardDAVErrorKind(err)
		span.SetStatus(codes.Error, outcome)
		ce.Reply("%s", cardDAVLoginErrorText(err))
		return
	}
	host := hostOf(creds.URL)
	span.SetAttributes(attribute.String("carddav.host", host), attribute.Int("carddav.books", len(books)))
	if err = saveCardDAVCreds(ctx, ul, creds); err != nil {
		outcome = "save_failed"
		span.SetStatus(codes.Error, outcome)
		ce.Reply("%s", "Connected, but saving failed: "+cardDAVSafeErr(err)+".")
		return
	}
	outcome = "ok"
	labels := make([]string, 0, len(books))
	for _, b := range books {
		labels = append(labels, bookLabel(b))
	}
	ce.Reply("%s", fmt.Sprintf("Connected to %s as %s: found %d address book(s) (%s). Saved. Run `%s carddav-sync` to preview what would be added to your roster.",
		host, maskCardDAVUser(creds.Username), len(books), strings.Join(labels, ", "), prefix))
}

// cardDAVLoginErrorText is the reply for a failed validation (host-only messages, no server text).
func cardDAVLoginErrorText(err error) string {
	if ce, ok := err.(*cardDAVError); ok {
		return ce.msg
	}
	return "Could not connect: " + cardDAVSafeErr(err) + "."
}

// cardDAVSafeErr is a short, classified description of an unexpected error (never raw server text).
func cardDAVSafeErr(err error) string {
	if _, ok := err.(*cardDAVError); ok {
		return err.Error()
	}
	return "unexpected error (" + cardDAVErrorKind(err) + ")"
}

// CardDAVLogoutCommand is the handler of `carddav-logout`.
func CardDAVLogoutCommand(ce *commands.Event) {
	ul, ok := cardDAVGate(ce, false)
	if !ok {
		return
	}
	if _, has := cardDAVCredsOf(loginMeta(ul)); !has {
		ce.Reply("No CardDAV account is set up for this login.")
		return
	}
	if err := saveCardDAVCreds(ce.Ctx, ul, nil); err != nil {
		ce.Reply("%s", "Removing the CardDAV account failed: "+cardDAVSafeErr(err)+".")
		return
	}
	ce.Reply("CardDAV account removed. Contacts already added to your roster stay.")
}

// CardDAVStatusCommand is the handler of `carddav-status`.
func CardDAVStatusCommand(ce *commands.Event) {
	ul, ok := cardDAVGate(ce, false)
	if !ok {
		return
	}
	creds, has := cardDAVCredsOf(loginMeta(ul))
	if !has {
		ce.Reply("%s", fmt.Sprintf("No CardDAV account is set up for this login. Use `%s carddav-login`.", cardDAVCmdPrefix(ce)))
		return
	}
	running, last := cardDAVRunStatus(ul.ID)
	var b strings.Builder
	fmt.Fprintf(&b, "CardDAV server: %s\nUser: %s\nAddress books: %d", hostOf(creds.URL), maskCardDAVUser(creds.Username), len(creds.Books))
	if creds.Book != "" {
		b.WriteString(" (selected by name)")
	}
	switch {
	case running:
		b.WriteString("\nA sync is running now.")
	case last == nil:
		b.WriteString("\nNo sync has run since the bridge started.")
	default:
		kind := "preview"
		if last.Apply {
			kind = "sync"
		}
		fmt.Fprintf(&b, "\nLast %s (%s): %d contacts read, %d numbers planned, %d added, %d unnamed entries planned to name, %d named, %d already in your roster, %d skipped, %d failed, outcome %s.",
			kind, last.At.Format(time.RFC3339), last.Read, last.Planned, last.Added, last.PlanNames, last.Named, last.Already, last.Skipped, last.Failed, last.Outcome)
	}
	ce.Reply("%s", b.String())
}

// CardDAVSyncCommand is the handler of `carddav-sync [apply]`: a preview by default, `apply` writes.
func CardDAVSyncCommand(ce *commands.Event) {
	prefix := cardDAVCmdPrefix(ce)
	apply := false
	switch {
	case len(ce.Args) == 0:
	case len(ce.Args) == 1 && strings.EqualFold(ce.Args[0], "apply"):
		apply = true
	default:
		ce.Reply("%s", fmt.Sprintf("Usage: `%s carddav-sync` previews what would be added to your roster; `%s carddav-sync apply` adds it.", prefix, prefix))
		return
	}
	ul, ok := cardDAVGate(ce, true)
	if !ok {
		return
	}
	creds, has := cardDAVCredsOf(loginMeta(ul))
	if !has {
		ce.Reply("%s", fmt.Sprintf("No CardDAV account is set up for this login. Use `%s carddav-login`.", prefix))
		return
	}
	x, isReal := ul.Client.(*xmppClient)
	if !isReal {
		ce.Reply("Log in to Jabber first.")
		return
	}
	if _, loaded := x.rosterSnapshot(); !loaded {
		ce.Reply("The roster has not loaded yet. Try again in a moment.")
		return
	}
	if !beginCardDAVRun(ul.ID) {
		ce.Reply("A CardDAV sync is already running for this login.")
		return
	}
	cfg := cardDAVConfigOf(ce.Bridge)
	// Everything the detached run needs is captured here: it must not touch ce (withSpan restores
	// ce.Ctx and ce.Log when the handler returns) except through this private copy.
	ev := *ce
	clean := ce.Bridge.Log.With().Str("component", "carddav").Logger()
	from := telemetry.CarrySpan(context.Background(), ce.Ctx)
	base := context.WithoutCancel(ce.Ctx)
	job := &cardDAVSyncJob{
		loginID: ul.ID, cfg: cfg, creds: creds, apply: apply, prefix: prefix,
		snapshot: x.rosterSnapshot, fetch: fetchContacts,
		env: execEnv{
			setItem:  x.SetRosterItem,
			rosterOf: x.rosterEntryName,
			loggedIn: x.IsLoggedIn,
			wait:     sleepCtx,
			perSec:   cardDAVAddsPerSecond,
			maxAdds:  cardDAVMaxAddsPerRun,
			maxFails: cardDAVMaxConsecutiveFailures,
		},
	}
	if apply {
		ce.Reply("CardDAV sync started. I will report back when it finishes.")
	} else {
		ce.Reply("CardDAV preview started. I will report back when it finishes.")
	}
	go func() {
		ctx, span := telemetry.StartLinkedRoot(base, "cmd.carddav_sync", from)
		defer span.End()
		log := telemetry.Logger(ctx, &clean)
		ev.Ctx, ev.Log = ctx, log
		job.log = log
		job.env.debug = func(jid string, fill bool) {
			log.Debug().Str("jid", jid).Bool("named_existing", fill).Msg("CardDAV roster write")
		}
		job.reply = func(text string) { ev.Reply("%s", text) }
		job.run(ctx, span)
	}()
}
