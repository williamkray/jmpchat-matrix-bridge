package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type fakeExec struct {
	set       []string
	inRoster  map[string]bool // named entries
	unnamed   map[string]bool // entries without a name
	failFor   map[string]error
	loggedIn  bool
	waits     []time.Duration
	cancelAt  int // cancel the context after this many set calls (0 = never)
	cancel    context.CancelFunc
	loggedOut int // flip loggedIn to false after this many set calls (0 = never)
}

func (f *fakeExec) env(maxAdds int) execEnv {
	return execEnv{
		setItem: func(_ context.Context, jid, name string) error {
			if err := f.failFor[jid]; err != nil {
				return err
			}
			f.set = append(f.set, jid+"="+name)
			if f.cancelAt > 0 && len(f.set) == f.cancelAt {
				f.cancel()
			}
			if f.loggedOut > 0 && len(f.set) == f.loggedOut {
				f.loggedIn = false
			}
			return nil
		},
		rosterOf: func(jid string) (string, bool) {
			if f.inRoster[jid] {
				return "Named", true
			}
			return "", f.unnamed[jid]
		},
		loggedIn: func() bool { return f.loggedIn },
		wait: func(ctx context.Context, d time.Duration) error {
			f.waits = append(f.waits, d)
			return ctx.Err()
		},
		perSec: 5, maxAdds: maxAdds, maxFails: 5,
	}
}

func adds(n int) []rosterAdd {
	var out []rosterAdd
	for i := 0; i < n; i++ {
		e := fmt.Sprintf("+1555000%04d", i)
		out = append(out, rosterAdd{JID: e + "@cheogram.com", Name: fmt.Sprintf("C%d", i), E164: e})
	}
	return out
}

func TestApplyPlanRateLimitAndMaxAdds(t *testing.T) {
	f := &fakeExec{loggedIn: true}
	res := applyPlan(context.Background(), f.env(3), adds(5))
	if res.Added != 3 || res.Outcome != outcomeMaxAdds || res.Remaining != 2 || len(f.set) != 3 {
		t.Fatalf("res = %+v set = %v", res, f.set)
	}
	// 5 per second: 200ms between adds, none before the first.
	if len(f.waits) != 2 || f.waits[0] != 200*time.Millisecond {
		t.Errorf("waits = %v", f.waits)
	}
}

func TestApplyPlanAbortsAfterConsecutiveFailures(t *testing.T) {
	f := &fakeExec{loggedIn: true, failFor: map[string]error{}}
	list := adds(10)
	for i := 1; i < 8; i++ { // add 0 succeeds, then 5 failures in a row
		f.failFor[list[i].JID] = errors.New("roster set failed: resource-constraint")
	}
	res := applyPlan(context.Background(), f.env(0), list)
	if res.Outcome != outcomeFailures || res.Added != 1 || res.Failed != 5 || res.FailRun != 5 || res.Remaining != 4 {
		t.Fatalf("res = %+v", res)
	}
	// A success resets the counter.
	f = &fakeExec{loggedIn: true, failFor: map[string]error{}}
	list = adds(12)
	for _, i := range []int{0, 1, 2, 3, 5, 6, 7, 8} {
		f.failFor[list[i].JID] = errors.New("boom")
	}
	res = applyPlan(context.Background(), f.env(0), list)
	if res.Outcome != outcomeDone || res.Failed != 8 || res.Added != 4 {
		t.Fatalf("res = %+v", res)
	}
}

func TestApplyPlanDisconnect(t *testing.T) {
	f := &fakeExec{loggedIn: true, failFor: map[string]error{}}
	list := adds(6)
	f.failFor[list[2].JID] = errors.New("not connected to Jabber")
	res := applyPlan(context.Background(), f.env(0), list)
	if res.Outcome != outcomeDisconnect || res.Added != 2 || res.Failed != 0 || res.Remaining != 4 {
		t.Fatalf("not connected error: %+v", res)
	}
	f = &fakeExec{loggedIn: true, loggedOut: 2}
	res = applyPlan(context.Background(), f.env(0), adds(6))
	if res.Outcome != outcomeDisconnect || res.Added != 2 || res.Remaining != 4 {
		t.Fatalf("logged out: %+v", res)
	}
}

func TestApplyPlanCancelAndRecheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeExec{loggedIn: true, cancelAt: 2, cancel: cancel}
	res := applyPlan(ctx, f.env(0), adds(6))
	if res.Outcome != outcomeCancelled || res.Added != 2 || res.Remaining != 4 {
		t.Fatalf("cancel: %+v", res)
	}
	list := adds(3)
	f = &fakeExec{loggedIn: true, inRoster: map[string]bool{list[1].JID: true}}
	res = applyPlan(context.Background(), f.env(0), list)
	if res.Added != 2 || res.Already != 1 || strings.Contains(strings.Join(f.set, ","), list[1].JID) {
		t.Fatalf("re-check must not set an entry that appeared: %+v %v", res, f.set)
	}
}

func TestCardDAVBusyGuard(t *testing.T) {
	id := "busy@example.com"
	if !beginCardDAVRun("busy@example.com") {
		t.Fatal("first run refused")
	}
	if beginCardDAVRun("busy@example.com") {
		t.Fatal("second concurrent run allowed")
	}
	if running, _ := cardDAVRunStatus("busy@example.com"); !running {
		t.Fatal("status should say running")
	}
	endCardDAVRun("busy@example.com", &cardDAVLastRun{Read: 3})
	if !beginCardDAVRun("busy@example.com") {
		t.Fatal("run after end refused")
	}
	endCardDAVRun("busy@example.com", nil)
	if _, last := cardDAVRunStatus("busy@example.com"); last == nil || last.Read != 3 {
		t.Fatalf("last = %+v (%s)", last, id)
	}
}

func TestApplyPlanBoundsEachAdd(t *testing.T) {
	f := &fakeExec{loggedIn: true}
	env := f.env(0)
	var deadlines int
	env.setItem = func(ctx context.Context, _, _ string) error {
		if _, ok := ctx.Deadline(); ok {
			deadlines++
		}
		return nil
	}
	if res := applyPlan(context.WithoutCancel(context.Background()), env, adds(2)); res.Added != 2 || deadlines != 2 {
		t.Fatalf("res = %+v, adds with a deadline = %d", res, deadlines)
	}
}
