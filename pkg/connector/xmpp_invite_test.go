package connector

import (
	"context"
	"encoding/xml"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const (
	invRoom    = "room@muc.example.com"
	invSecretR = "SECRETREASON"
	invSecretP = "SECRETPASS"
)

func mediatedInvite(extra string) string {
	return `<message from="` + invRoom + `" to="me@example.com" id="i1"><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com/res"><reason>` + invSecretR + `</reason></invite><password>` + invSecretP + `</password></x>` + extra + `</message>`
}

func directInvite(extra string) string {
	return `<message from="bob@example.com/res" to="me@example.com" id="i2"><x xmlns="jabber:x:conference" jid="` + invRoom + `" password="` + invSecretP + `" reason="` + invSecretR + `"/>` + extra + `</message>`
}

func parseInvite(t *testing.T, stanza string) *mucInvite {
	t.Helper()
	r, start := startOfMessage(t, stanza)
	_, _, _, _, _, _, ids, err := parseMessageBodyOOBIDs(r)
	if err != nil {
		t.Fatal(err)
	}
	return ids.inviteFrom(attrValue(start.Attr, "from"))
}

func TestParseInviteForms(t *testing.T) {
	m := parseInvite(t, mediatedInvite(`<body>fallback</body>`))
	if m == nil || m.room != invRoom || m.inviter != "bob@example.com" || m.reason != invSecretR || m.password != invSecretP || m.direct {
		t.Fatalf("mediated: %+v", m)
	}
	d := parseInvite(t, directInvite(`<body>fallback</body>`))
	if d == nil || d.room != invRoom || d.inviter != "bob@example.com" || d.reason != invSecretR || d.password != invSecretP || !d.direct {
		t.Fatalf("direct: %+v", d)
	}
}

func TestParseInviteRejects(t *testing.T) {
	for name, s := range map[string]string{
		"bad room":               `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="not a jid"/></message>`,
		"room with resource":     `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="room@muc.example.com/nick"/></message>`,
		"room without local":     `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="muc.example.com"/></message>`,
		"oversized room":         `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="` + strings.Repeat("a", 300) + `@example.com"/></message>`,
		"nested not direct":      `<message from="bob@example.com/r"><wrap><x xmlns="jabber:x:conference" jid="room@muc.example.com"/></wrap></message>`,
		"nested mediated":        `<message from="room@muc.example.com"><wrap><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com"/></x></wrap></message>`,
		"mediated from occupant": `<message from="room@muc.example.com/nick"><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com"/></x></message>`,
		"status only":            `<message from="room@muc.example.com"><x xmlns="http://jabber.org/protocol/muc#user"><status code="110"/></x></message>`,
	} {
		if m := parseInvite(t, s); m != nil {
			t.Errorf("%s: parsed %+v", name, m)
		}
	}
}

func TestParseInviteCaps(t *testing.T) {
	m := parseInvite(t, `<message from="bob@example.com/r"><x xmlns="jabber:x:conference" jid="`+invRoom+`" password="`+strings.Repeat("p", 300)+`" reason="`+strings.Repeat("r", 5000)+`"/></message>`)
	if m == nil || m.password != "" || len(m.reason) != maxInviteReasonBytes {
		t.Fatalf("caps: %+v", m)
	}
	m = parseInvite(t, `<message from="`+invRoom+`"><x xmlns="http://jabber.org/protocol/muc#user"><invite from="bob@example.com"><reason>`+strings.Repeat("r", 5000)+`</reason></invite></x></message>`)
	if m == nil || len(m.reason) != maxInviteReasonBytes {
		t.Fatalf("mediated caps: %+v", m)
	}
}

type inviteRig struct {
	x       *xmppClient
	mu      sync.Mutex
	notices []string
	events  []bridgev2.RemoteEvent
	joins   []string
	left    []bool
	portals []*database.Portal
	done    chan struct{}
	logs    *bmLogBuf
	member  event.Membership
	known   bool
}

func newInviteRig() *inviteRig {
	x, _ := hookClient()
	r := &inviteRig{x: x, done: make(chan struct{}, 32), logs: &bmLogBuf{}}
	x.log = zerolog.New(r.logs).Level(zerolog.TraceLevel)
	x.mucNoticeHook = func(_, text string) {
		r.mu.Lock()
		r.notices = append(r.notices, text)
		r.mu.Unlock()
		r.done <- struct{}{}
	}
	x.queueHook = func(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
		r.mu.Lock()
		r.events = append(r.events, evt)
		r.mu.Unlock()
		r.done <- struct{}{}
		return bridgev2.EventHandlingResultSuccess
	}
	x.mucRejoinPortalsHook = func(context.Context) ([]*database.Portal, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.portals, nil
	}
	x.inviteMembershipHook = func(id.RoomID) (event.Membership, bool) { return r.member, r.known }
	x.mucSendJoinHook = func(room, _ string) error {
		r.mu.Lock()
		r.joins = append(r.joins, room)
		r.mu.Unlock()
		return nil
	}
	x.mucLeftHook = func(_ string, left bool) error {
		r.mu.Lock()
		r.left = append(r.left, left)
		r.mu.Unlock()
		return nil
	}
	return r
}

func (r *inviteRig) wait(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the invite action")
	}
}

// settle waits briefly and returns the number of queued events and notices.
func (r *inviteRig) settle() (events, notices int) {
	time.Sleep(50 * time.Millisecond)
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events), len(r.notices)
}

func (r *inviteRig) addPortal(mxid id.RoomID, meta *PortalMetadata) {
	r.mu.Lock()
	r.portals = append(r.portals, &database.Portal{
		PortalKey: portalKeyFor(invRoom, r.x.loginID), MXID: mxid, Metadata: meta,
	})
	r.mu.Unlock()
}

func (r *inviteRig) resync(t *testing.T) *simplevent.ChatResync {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) != 1 {
		t.Fatalf("queued %d events, want 1", len(r.events))
	}
	e, ok := r.events[0].(*simplevent.ChatResync)
	if !ok {
		t.Fatalf("queued %T, want *simplevent.ChatResync", r.events[0])
	}
	return e
}

// chatInfoOf runs the resync's info func against a portal (as bridgev2 does) and checks the member list.
func (r *inviteRig) chatInfoOf(t *testing.T, e *simplevent.ChatResync, p *bridgev2.Portal) *bridgev2.ChatInfo {
	t.Helper()
	info, err := e.GetChatInfoFunc(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if info.Members == nil || info.Members.IsFull || len(info.Members.MemberMap) != 1 {
		t.Fatalf("members = %+v", info.Members)
	}
	for _, m := range info.Members.MemberMap {
		if m.Membership != event.MembershipInvite || m.EventSender.Sender != "me@example.com" {
			t.Fatalf("member = %+v, want the user invited", m)
		}
	}
	return info
}

func portalKeyFor(room string, login networkid.UserLoginID) networkid.PortalKey {
	return networkid.PortalKey{ID: networkid.PortalID(room), Receiver: login}
}

func newInvitePortal(mxid id.RoomID, meta *PortalMetadata) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{PortalKey: portalKeyFor(invRoom, "me@example.com"), MXID: mxid, Metadata: meta}}
}

func TestInviteNewRoomCreatesInvitedPortalWithoutJoining(t *testing.T) {
	r := newInviteRig()
	x := r.x
	runHandler(t, x, mediatedInvite(`<body>FALLBACKBODY</body>`))
	r.wait(t)
	// The same room invited through the other form inside 10 minutes: deduped.
	runHandler(t, x, directInvite(`<body>FALLBACKBODY</body>`))
	if ev, n := r.settle(); ev != 1 || n != 0 {
		t.Fatalf("events=%d notices=%d, want 1 and 0 before the room exists", ev, n)
	}
	e := r.resync(t)
	if !e.CreatePortal || e.PortalKey.ID != invRoom || e.PortalKey.Receiver != x.loginID {
		t.Fatalf("resync meta: %+v", e.EventMeta)
	}
	portal := newInvitePortal("", &PortalMetadata{})
	info := r.chatInfoOf(t, e, portal)
	if info.Name == nil || info.Type == nil || *info.Type != database.RoomTypeGroupDM {
		t.Fatalf("info lacks name/type: %+v", info)
	}
	if !info.ExtraUpdates(context.Background(), portal) {
		t.Error("ExtraUpdates reported no change")
	}
	meta := portal.Metadata.(*PortalMetadata)
	if !meta.MUCLeft || !meta.MUCInvitePending {
		t.Errorf("portal not marked left+pending: %+v", meta)
	}
	// Notice goes into the new portal room once it exists (post-handler).
	portal.MXID = "!new:example.com"
	e.PostHandleFunc(context.Background(), portal)
	r.mu.Lock()
	text := r.notices[0]
	joins := len(r.joins)
	r.mu.Unlock()
	for _, want := range []string{"bob@example.com", invRoom, invSecretR, "Accept the invite", "password"} {
		if !strings.Contains(text, want) {
			t.Errorf("notice %q lacks %q", text, want)
		}
	}
	if strings.Contains(text, invSecretP) || strings.Contains(text, "join "+invRoom) {
		t.Errorf("notice leaks the password or still names the join command: %q", text)
	}
	if joins != 0 {
		t.Fatal("a join presence was sent for an invite")
	}
	if out := r.logs.String(); strings.Contains(out, invSecretP) || strings.Contains(out, invSecretR) || strings.Contains(out, "FALLBACKBODY") {
		t.Errorf("logs leak invite content:\n%s", out)
	}
}

func TestInviteExistingLeftRoomNotJoinedReinvites(t *testing.T) {
	for _, m := range []event.Membership{event.MembershipLeave, event.MembershipInvite} {
		r := newInviteRig()
		r.addPortal("!old:example.com", &PortalMetadata{MUCLeft: true})
		r.member, r.known = m, true
		runHandler(t, r.x, directInvite(""))
		r.wait(t)
		e := r.resync(t)
		if e.CreatePortal {
			t.Errorf("%s: re-invite must not create a portal", m)
		}
		portal := newInvitePortal("!old:example.com", &PortalMetadata{MUCLeft: true})
		info := r.chatInfoOf(t, e, portal)
		info.ExtraUpdates(context.Background(), portal)
		if meta := portal.Metadata.(*PortalMetadata); !meta.MUCLeft || !meta.MUCInvitePending {
			t.Errorf("%s: not pending: %+v", m, meta)
		}
		if len(r.joins) != 0 {
			t.Error("join sent")
		}
	}
}

func TestInviteExistingJoinedRoomPostsNotice(t *testing.T) {
	for name, known := range map[string]bool{"joined": true, "unknown": false} {
		r := newInviteRig()
		r.addPortal("!old:example.com", &PortalMetadata{MUCLeft: true})
		r.member, r.known = event.MembershipJoin, known
		runHandler(t, r.x, directInvite(""))
		r.wait(t)
		if name == "unknown" {
			// Unknown membership: the bridge cannot tell, so it must not re-invite; it falls back to the notice.
			if ev, n := r.settle(); ev != 0 || n != 1 {
				t.Fatalf("%s: events=%d notices=%d", name, ev, n)
			}
		}
		ev, n := r.settle()
		if ev != 0 || n != 1 {
			t.Fatalf("%s: events=%d notices=%d, want 0 and 1", name, ev, n)
		}
		if txt := r.notices[0]; !strings.Contains(txt, "!jabber join "+invRoom) || !strings.Contains(txt, "bob@example.com") {
			t.Errorf("%s: notice %q", name, txt)
		}
	}
}

func TestInviteExistingActiveRoomIgnored(t *testing.T) {
	r := newInviteRig()
	r.addPortal("!old:example.com", &PortalMetadata{})
	runHandler(t, r.x, directInvite(""))
	if ev, n := r.settle(); ev != 0 || n != 0 {
		t.Fatalf("events=%d notices=%d", ev, n)
	}
}

func TestInviteHourlyCap(t *testing.T) {
	r := newInviteRig()
	for i := 0; i < maxInvitesPerHour+5; i++ {
		room := "r" + string(rune('a'+i)) + "@muc.example.com"
		runHandler(t, r.x, `<message from="bob@example.com/res"><x xmlns="jabber:x:conference" jid="`+room+`"/></message>`)
	}
	if ev, _ := r.settle(); ev != maxInvitesPerHour {
		t.Fatalf("acted on %d invites, want %d", ev, maxInvitesPerHour)
	}
	// An hour later the window is free again.
	r.x.inviteMu.Lock()
	for i := range r.x.inviteActed {
		r.x.inviteActed[i] = time.Now().Add(-inviteCapWindow - time.Second)
	}
	r.x.inviteMu.Unlock()
	runHandler(t, r.x, `<message from="bob@example.com/res"><x xmlns="jabber:x:conference" jid="late@muc.example.com"/></message>`)
	r.wait(t)
	if ev, _ := r.settle(); ev != maxInvitesPerHour+1 {
		t.Fatalf("after the window: %d events", ev)
	}
}

func TestInvitePasswordUsedByJoin(t *testing.T) {
	r := newInviteRig()
	x := r.x
	runHandler(t, x, mediatedInvite(""))
	r.wait(t)
	var toks []xml.Token
	x.mucJoinTokensHook = func(_ string, tk []xml.Token) { toks = tk }
	if err := x.sendMUCJoin(context.Background(), invRoom, "me", nil); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tk := range toks {
		if cd, ok := tk.(xml.CharData); ok && string(cd) == invSecretP {
			found = true
		}
	}
	if !found {
		t.Error("join presence lacks the invite password")
	}
	x.resetInvites()
	if x.invitePassword(invRoom) != "" {
		t.Error("invite password survived a session reset")
	}
}

func membershipChange(x *xmppClient, typ bridgev2.MembershipChangeType, meta *PortalMetadata) *bridgev2.MatrixMembershipChange {
	return &bridgev2.MatrixMembershipChange{
		MatrixRoomMeta: bridgev2.MatrixRoomMeta[*event.MemberEventContent]{
			MatrixEventBase: bridgev2.MatrixEventBase[*event.MemberEventContent]{Portal: newInvitePortal("!new:example.com", meta)},
		},
		Target: x.login,
		Type:   typ,
	}
}

func TestHandleMatrixMembershipAcceptJoinsWithPassword(t *testing.T) {
	r := newInviteRig()
	x := r.x
	runHandler(t, x, mediatedInvite(""))
	r.wait(t)
	var toks []xml.Token
	x.mucJoinTokensHook = func(_ string, tk []xml.Token) { toks = tk }
	x.session = nil
	res, err := x.HandleMatrixMembership(context.Background(), membershipChange(x, bridgev2.AcceptInvite, &PortalMetadata{MUCLeft: true, MUCInvitePending: true}))
	if err != nil || res != nil {
		t.Fatalf("res=%v err=%v", res, err)
	}
	r.mu.Lock()
	joins, left := append([]string(nil), r.joins...), append([]bool(nil), r.left...)
	r.mu.Unlock()
	if len(joins) != 1 || joins[0] != invRoom {
		t.Fatalf("joins = %v", joins)
	}
	if len(left) != 1 || left[0] {
		t.Fatalf("left flag calls = %v, want one clear", left)
	}
	found := false
	for _, tk := range toks {
		if cd, ok := tk.(xml.CharData); ok && string(cd) == invSecretP {
			found = true
		}
	}
	if !found {
		t.Error("accept join lacks the invite password")
	}
}

func TestHandleMatrixMembershipOtherTypes(t *testing.T) {
	r := newInviteRig()
	x := r.x
	pending := &PortalMetadata{MUCLeft: true, MUCInvitePending: true}
	// Reject: nothing on XMPP, password forgotten.
	x.inviteMu.Lock()
	x.invitePasswords = map[string]string{invRoom: invSecretP}
	x.inviteMu.Unlock()
	if _, err := x.HandleMatrixMembership(context.Background(), membershipChange(x, bridgev2.RejectInvite, pending)); err != nil {
		t.Fatal(err)
	}
	if x.invitePassword(invRoom) != "" {
		t.Error("rejected invite kept its password")
	}
	// An ordinary join (not pending): no XMPP action.
	if _, err := x.HandleMatrixMembership(context.Background(), membershipChange(x, bridgev2.AcceptInvite, &PortalMetadata{})); err != nil {
		t.Fatal(err)
	}
	// Own profile change: harmless.
	if _, err := x.HandleMatrixMembership(context.Background(), membershipChange(x, bridgev2.ProfileChange, nil)); err != nil {
		t.Fatal(err)
	}
	// Everything else keeps the old "not supported" answer; leaving a Matrix room never leaves the MUC.
	for _, typ := range []bridgev2.MembershipChangeType{bridgev2.Leave, bridgev2.Kick, bridgev2.Invite, bridgev2.BanJoined, bridgev2.RevokeInvite} {
		if _, err := x.HandleMatrixMembership(context.Background(), membershipChange(x, typ, pending)); !errors.Is(err, bridgev2.ErrMembershipNotSupported) {
			t.Errorf("%+v: err = %v", typ, err)
		}
	}
	// An accept targeting another account's login is not ours to handle.
	other := membershipChange(x, bridgev2.AcceptInvite, pending)
	other.Target = &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "other@example.com"}}
	if _, err := x.HandleMatrixMembership(context.Background(), other); !errors.Is(err, bridgev2.ErrMembershipNotSupported) {
		t.Errorf("other login: err = %v", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.joins) != 0 || len(r.left) != 0 {
		t.Fatalf("joins=%v left=%v, want none", r.joins, r.left)
	}
}

func TestApplyMUCLeftClearsPending(t *testing.T) {
	m := &PortalMetadata{MUCLeft: true, MUCInvitePending: true}
	if !applyMUCLeft(m, false) || m.MUCLeft || m.MUCInvitePending {
		t.Fatalf("clear: %+v", m)
	}
	if applyMUCLeft(m, false) {
		t.Error("second clear reported a change")
	}
	if !applyMUCLeft(m, true) || !m.MUCLeft || m.MUCInvitePending {
		t.Fatalf("set: %+v", m)
	}
	// A pending portal with MUCLeft already cleared still reports a change so the flag goes away.
	m = &PortalMetadata{MUCInvitePending: true}
	if !applyMUCLeft(m, false) || m.MUCInvitePending {
		t.Fatalf("pending only: %+v", m)
	}
}

func TestPendingPortalChatInfoKeepsUserInvited(t *testing.T) {
	r := newInviteRig()
	x := r.x
	// Before the Matrix room exists: invited. After: no member list at all (a resync cannot join the user).
	info, err := x.getMUCChatInfo(context.Background(), newInvitePortal("", &PortalMetadata{MUCInvitePending: true}), invRoom)
	if err != nil || info.Members == nil {
		t.Fatalf("members=%v err=%v", info, err)
	}
	for _, m := range info.Members.MemberMap {
		if m.Membership != event.MembershipInvite {
			t.Errorf("membership = %s", m.Membership)
		}
	}
	info, _ = x.getMUCChatInfo(context.Background(), newInvitePortal("!x:example.com", &PortalMetadata{MUCInvitePending: true}), invRoom)
	if info.Members != nil {
		t.Error("pending portal with a room still syncs members")
	}
	info, _ = x.getMUCChatInfo(context.Background(), newInvitePortal("!x:example.com", &PortalMetadata{MUCLeft: true}), invRoom)
	if info.Members == nil {
		t.Error("an ordinary left room lost its member list")
	}
	_ = r
}

func TestInviteIgnoredWhenJoinedOrCarbonOrMAM(t *testing.T) {
	r := newInviteRig()
	x := r.x
	x.mucOurOccupantJID = map[string]string{invRoom: invRoom + "/me"}
	runHandler(t, x, mediatedInvite(""))
	if ev, n := r.settle(); ev+n != 0 {
		t.Fatalf("joined room acted %d/%d times", ev, n)
	}
	x.mucOurOccupantJID = nil
	// Received carbon of the same invite: never acts.
	runHandler(t, x, `<message from="me@example.com" to="me@example.com/r"><received xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="bob@example.com/res" to="me@example.com" type="normal"><x xmlns="jabber:x:conference" jid="`+invRoom+`"/><body>FALLBACKBODY</body></message></forwarded></received></message>`)
	if ev, n := r.settle(); ev+n != 0 {
		t.Fatalf("carbon acted %d/%d times", ev, n)
	}
	// Invite from our own account is ignored too, as is a Cheogram number JID.
	runHandler(t, x, `<message from="me@example.com/other"><x xmlns="jabber:x:conference" jid="`+invRoom+`"/></message>`)
	runHandler(t, x, `<message from="bob@example.com/res"><x xmlns="jabber:x:conference" jid="+15551234567@cheogram.com"/></message>`)
	if ev, n := r.settle(); ev+n != 0 {
		t.Fatalf("own/cheogram invite acted %d/%d times", ev, n)
	}
}

func TestSanitizeInviteReason(t *testing.T) {
	got := sanitizeInviteReason("a\x00b\n*bold* [x](http://e)‮" + strings.Repeat("z", 400))
	if strings.ContainsAny(got, "\x00\n‮") || strings.Contains(got, "*bold*") || !strings.HasSuffix(got, "...") {
		t.Errorf("sanitize = %q", got)
	}
}
