package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	cardDAVMaxConsecutiveFailures = 5
	cardDAVPreviewLines           = 20
	cardDAVRosterSetTimeout       = 30 * time.Second // one roster add (IQ round trip plus refresh)
)

// Executor outcomes.
const (
	outcomeDone        = "ok"
	outcomeMaxAdds     = "max_adds"
	outcomeFailures    = "consecutive_failures"
	outcomeDisconnect  = "disconnected"
	outcomeCancelled   = "cancelled"
	outcomeFetchFailed = "fetch_failed"
)

// execEnv is what the executor needs from the XMPP client (a seam for tests).
type execEnv struct {
	setItem  func(ctx context.Context, jid, name string) error
	rosterOf func(jid string) (name string, in bool) // current roster entry (re-check before each write)
	loggedIn func() bool
	wait     func(ctx context.Context, d time.Duration) error // rate limiter sleep
	debug    func(jid string, fill bool)                      // per-write log (JID only)
	perSec   int
	maxAdds  int
	maxFails int
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// applyResult is what applyPlan did.
type applyResult struct {
	Added     int
	Named     int // unnamed roster entries that got a name
	Failed    int
	Already   int // in the roster (with a name) by the time of the re-check
	Gone      int // planned names whose entry was removed from the roster meanwhile (not re-added)
	Remaining int // planned writes not attempted (max adds, early stop)
	Outcome   string
	FailRun   int // consecutive failures when it stopped
}

// applyPlan writes the planned entries serially (rate limited): adds, and names for unnamed entries
// (Fill). Per-write failures are counted; it stops after maxFails consecutive failures, when the
// connection is gone, at maxAdds writes and on cancellation. A "not connected" failure stops at once
// without counting towards the consecutive-failure rule.
func applyPlan(ctx context.Context, env execEnv, adds []rosterAdd) applyResult {
	res := applyResult{Outcome: outcomeDone}
	interval := time.Second
	if env.perSec > 0 {
		interval = time.Second / time.Duration(env.perSec)
	}
	attempted, consecutive := 0, 0
	for i, a := range adds {
		if ctx.Err() != nil {
			res.Outcome, res.Remaining = outcomeCancelled, len(adds)-i
			return res
		}
		if !env.loggedIn() {
			res.Outcome, res.Remaining = outcomeDisconnect, len(adds)-i
			return res
		}
		// Re-check: a concurrent add or rename wins; a name is only set while the entry still exists
		// without one (a removal meanwhile is not undone).
		cur, in := env.rosterOf(a.JID)
		switch {
		case in && strings.TrimSpace(cur) != "":
			res.Already++
			continue
		case a.Fill && !in:
			res.Gone++
			continue
		case !a.Fill && in:
			// Added meanwhile without a name: name it like a planned fill.
			a.Fill = true
		}
		if env.maxAdds > 0 && attempted >= env.maxAdds {
			res.Outcome, res.Remaining = outcomeMaxAdds, len(adds)-i
			return res
		}
		if attempted > 0 {
			if err := env.wait(ctx, interval); err != nil {
				res.Outcome, res.Remaining = outcomeCancelled, len(adds)-i
				return res
			}
		}
		attempted++
		// Bounded: the run's ctx is detached and never cancelled, so an unanswered roster IQ would
		// otherwise keep the login's sync marked busy until restart.
		setCtx, cancel := context.WithTimeout(ctx, cardDAVRosterSetTimeout)
		err := env.setItem(setCtx, a.JID, a.Name)
		cancel()
		if err != nil {
			if strings.Contains(err.Error(), "not connected") {
				res.Outcome, res.Remaining = outcomeDisconnect, len(adds)-i
				return res
			}
			res.Failed++
			consecutive++
			if env.maxFails > 0 && consecutive >= env.maxFails {
				res.Outcome, res.FailRun, res.Remaining = outcomeFailures, consecutive, len(adds)-i-1
				return res
			}
			continue
		}
		consecutive = 0
		if a.Fill {
			res.Named++
		} else {
			res.Added++
		}
		if env.debug != nil {
			env.debug(a.JID, a.Fill)
		}
	}
	return res
}

// cardDAVLastRun is the in-memory summary of a login's last sync (lost on restart).
type cardDAVLastRun struct {
	At          time.Time
	Apply       bool
	Read        int
	Added       int
	Named       int
	Planned     int // planned adds
	PlanNames   int // planned names for unnamed entries
	Already     int
	Skipped     int
	Failed      int
	BooksFailed int
	Outcome     string
}

type cardDAVRunState struct {
	running bool
	last    *cardDAVLastRun
}

var cardDAVRuns = struct {
	sync.Mutex
	m map[networkid.UserLoginID]*cardDAVRunState
}{m: map[networkid.UserLoginID]*cardDAVRunState{}}

// beginCardDAVRun marks a run as started; false if one is already running for the login.
func beginCardDAVRun(id networkid.UserLoginID) bool {
	cardDAVRuns.Lock()
	defer cardDAVRuns.Unlock()
	st := cardDAVRuns.m[id]
	if st == nil {
		st = &cardDAVRunState{}
		cardDAVRuns.m[id] = st
	}
	if st.running {
		return false
	}
	st.running = true
	return true
}

func endCardDAVRun(id networkid.UserLoginID, last *cardDAVLastRun) {
	cardDAVRuns.Lock()
	defer cardDAVRuns.Unlock()
	if st := cardDAVRuns.m[id]; st != nil {
		st.running = false
		if last != nil {
			st.last = last
		}
	}
}

func cardDAVRunStatus(id networkid.UserLoginID) (running bool, last *cardDAVLastRun) {
	cardDAVRuns.Lock()
	defer cardDAVRuns.Unlock()
	if st := cardDAVRuns.m[id]; st != nil {
		c := *st
		if c.last != nil {
			l := *c.last
			return c.running, &l
		}
		return c.running, nil
	}
	return false, nil
}

// cardDAVSyncJob is one detached preview or apply run. It captures everything it needs; it never uses
// the command event of the handler that started it.
type cardDAVSyncJob struct {
	loginID  networkid.UserLoginID
	cfg      CardDAVConfig
	creds    *cardDAVCreds
	apply    bool
	prefix   string
	snapshot func() (map[string]string, bool)
	env      execEnv
	fetch    func(ctx context.Context, cfg CardDAVConfig, creds *cardDAVCreds) (*fetchSummary, error)
	reply    func(text string)
	log      *zerolog.Logger
}

// run executes the job and always ends the busy marker. span is the job's own root span.
func (j *cardDAVSyncJob) run(ctx context.Context, span trace.Span) {
	last := &cardDAVLastRun{At: time.Now(), Apply: j.apply, Outcome: outcomeFetchFailed}
	defer func() {
		endCardDAVRun(j.loginID, last)
		span.SetAttributes(
			attribute.Bool("carddav.apply", j.apply),
			attribute.Int("carddav.read", last.Read),
			attribute.Int("carddav.planned", last.Planned),
			attribute.Int("carddav.added", last.Added),
			attribute.Int("carddav.plan_names", last.PlanNames),
			attribute.Int("carddav.named", last.Named),
			attribute.Int("carddav.already", last.Already),
			attribute.Int("carddav.skipped", last.Skipped),
			attribute.Int("carddav.failed", last.Failed),
			attribute.Int("carddav.books_failed", last.BooksFailed),
			attribute.String("carddav.outcome", last.Outcome),
		)
		if last.Outcome != outcomeDone && last.Outcome != outcomeMaxAdds {
			span.SetStatus(codes.Error, last.Outcome)
		}
	}()
	host := hostOf(j.creds.URL)
	span.SetAttributes(attribute.String("carddav.host", host))
	s, err := j.fetch(ctx, j.cfg, j.creds)
	if err != nil {
		last.Outcome = cardDAVErrorKind(err)
		j.log.Info().Str("host", host).Str("outcome", last.Outcome).Msg("CardDAV sync failed")
		j.reply(fmt.Sprintf("CardDAV sync failed: %s", err.Error()))
		return
	}
	last.BooksFailed = s.BooksFailed
	roster, loaded := j.snapshot()
	if !loaded {
		last.Outcome = "roster_not_loaded"
		j.reply("The roster has not loaded yet. Try again in a moment.")
		return
	}
	plan := planCardDAVSync(s.Contacts, roster, s.Skips)
	last.Read, last.Planned, last.PlanNames, last.Already, last.Skipped = plan.Read, len(plan.Adds), len(plan.Names), plan.AlreadyInRoster, totalSkipped(plan.Skipped)
	notes := j.fetchNotes(s)
	if !j.apply {
		last.Outcome = outcomeDone
		j.log.Info().Str("host", host).Int("read", plan.Read).Int("planned", len(plan.Adds)).Int("plan_names", len(plan.Names)).
			Int("already", plan.AlreadyInRoster).Int("skipped", last.Skipped).Msg("CardDAV preview finished")
		j.reply(j.previewText(plan) + notes)
		return
	}
	res := applyPlan(ctx, j.env, plan.writes())
	last.Added, last.Named, last.Failed, last.Already, last.Outcome = res.Added, res.Named, res.Failed, last.Already+res.Already, res.Outcome
	j.log.Info().Str("host", host).Int("read", plan.Read).Int("added", res.Added).Int("named", res.Named).Int("failed", res.Failed).
		Int("already", last.Already).Int("gone", res.Gone).Int("skipped", last.Skipped).Str("outcome", res.Outcome).Msg("CardDAV sync finished")
	j.reply(j.summaryText(plan, res) + notes)
}

func hostOf(rawURL string) string {
	if u, err := normalizeCardDAVURL(rawURL, CardDAVConfig{AllowInsecureHTTP: true}); err == nil {
		return u.Hostname()
	}
	return ""
}

// fetchNotes mentions books that failed and truncated answers.
func (j *cardDAVSyncJob) fetchNotes(s *fetchSummary) string {
	var b strings.Builder
	if s.BooksFailed > 0 {
		fmt.Fprintf(&b, "\n%d address book(s) could not be read and were skipped (the others were synced).", s.BooksFailed)
	}
	if s.Truncated {
		b.WriteString("\nThe server said its answer was truncated; some contacts may be missing.")
	}
	return b.String()
}

func (j *cardDAVSyncJob) previewText(p cardDAVPlan) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CardDAV preview: %d contacts read, %d numbers would be added,%s %d already in your roster, %d skipped%s.",
		p.Read, len(p.Adds), namesClause(len(p.Names), "would get a name"), p.AlreadyInRoster, totalSkipped(p.Skipped), reasonSuffix(p.Skipped))
	writes := p.writes()
	if len(writes) == 0 {
		b.WriteString("\nNothing to add.")
		return b.String()
	}
	n := min(len(writes), cardDAVPreviewLines)
	for _, a := range writes[:n] {
		fmt.Fprintf(&b, "\n- %s: %s (%s)", a.Name, a.E164, a.Label)
		if a.Fill {
			b.WriteString(", names the existing roster entry")
		}
	}
	if len(writes) > n {
		fmt.Fprintf(&b, "\n...and %d more.", len(writes)-n)
	}
	if len(writes) > cardDAVMaxAddsPerRun {
		fmt.Fprintf(&b, "\nOne run makes at most %d roster changes; run it again to continue.", cardDAVMaxAddsPerRun)
	}
	fmt.Fprintf(&b, "\nRun `%s carddav-sync apply` to apply them.", j.prefix)
	return b.String()
}

func (j *cardDAVSyncJob) summaryText(p cardDAVPlan, r applyResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CardDAV sync finished: %d contacts read, %d numbers added,%s %d already in your roster, %d skipped%s.",
		p.Read, r.Added, namesClause(r.Named, "named"), p.AlreadyInRoster+r.Already, totalSkipped(p.Skipped), reasonSuffix(p.Skipped))
	if r.Failed > 0 {
		fmt.Fprintf(&b, " %d failed.", r.Failed)
	}
	if r.Gone > 0 {
		fmt.Fprintf(&b, " %d not named because they were removed from your roster meanwhile.", r.Gone)
	}
	switch r.Outcome {
	case outcomeFailures:
		fmt.Fprintf(&b, "\nStopped early: the server refused %d roster changes in a row.", r.FailRun)
	case outcomeDisconnect:
		b.WriteString("\nStopped early: the Jabber connection was lost. Run it again once reconnected.")
	case outcomeCancelled:
		b.WriteString("\nStopped early: the bridge is shutting down. Run it again to continue.")
	case outcomeMaxAdds:
		fmt.Fprintf(&b, "\nMade %d roster changes; run it again to continue.", r.Added+r.Named)
	}
	return b.String()
}

// namesClause is the " N unnamed roster entries <verb>," part of a summary ("" when n is 0).
func namesClause(n int, verb string) string {
	if n == 0 {
		return ""
	}
	if n == 1 {
		return fmt.Sprintf(" 1 unnamed roster entry %s,", verb)
	}
	return fmt.Sprintf(" %d unnamed roster entries %s,", n, verb)
}

func reasonSuffix(skipped map[string]int) string {
	if s := skippedSummary(skipped); s != "" {
		return " (" + s + ")"
	}
	return ""
}
