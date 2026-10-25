package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"
)

// ---- ref plan and ownership validator ----

func TestRefLookupPlanCorrection(t *testing.T) {
	want := []refStep{{direct: true}, {keys: []string{"origin_id", "xmpp_message_id"}}, {keys: []string{"stanza_id"}}}
	for _, k := range []refKind{refCorrectionDM, refCorrectionMUC} {
		if got := refLookupPlan(k); !reflect.DeepEqual(got, want) {
			t.Errorf("plan(%d) = %+v, want %+v", k, got, want)
		}
	}
}

// validateOwnership is the non-moderation part of validateRetraction: both must agree on every case.
func TestValidateOwnershipMatchesRetractionRules(t *testing.T) {
	dm := portalFacts{key: dmPortal(), loginID: retractTestLogin}
	muc := portalFacts{key: mucPortal(), loginID: retractTestLogin, isMUC: true, ourOccupantFull: "room@muc.example.com/me"}
	mms := portalFacts{key: networkid.PortalKey{ID: "+15550001111,+15550002222@cheogram.com", Receiver: retractTestLogin}, loginID: retractTestLogin, isGroupMMS: true}
	rows := map[string]*database.Message{
		"bob":      row(dmPortal(), "m1", "bob@example.com", nil),
		"ours":     row(dmPortal(), "m2", "me@example.com", nil),
		"alice":    row(mucPortal(), "s1", "room@muc.example.com/alice", &MessageMetadata{StanzaID: "S1"}),
		"aliceOcc": row(mucPortal(), "s2", "room@muc.example.com/alice", &MessageMetadata{StanzaID: "S2", OccupantID: "OCC-A"}),
		"ourMUC":   row(mucPortal(), "s3", "me@example.com", &MessageMetadata{StanzaID: "S3"}),
		"mms":      row(mms.key, "g1", "+15550001111@cheogram.com", nil),
		"other":    row(networkid.PortalKey{ID: "other@example.com", Receiver: retractTestLogin}, "x", "bob@example.com", nil),
	}
	for _, tc := range []struct {
		name   string
		r      retraction
		p      portalFacts
		target string
	}{
		{"dm", retraction{channel: retractChanLive, msgType: "chat", fromFull: "bob@example.com/p"}, dm, "bob"},
		{"dm other sender", retraction{channel: retractChanLive, msgType: "chat", fromFull: "mallory@example.com/p"}, dm, "bob"},
		{"dm bad type", retraction{channel: retractChanLive, msgType: "headline", fromFull: "bob@example.com"}, dm, "bob"},
		{"carbon sent own", retraction{channel: retractChanCarbonSent, msgType: "chat", fromFull: "me@example.com/l"}, dm, "ours"},
		{"carbon sent not own", retraction{channel: retractChanCarbonSent, msgType: "chat", fromFull: "bob@example.com/p"}, dm, "ours"},
		{"carbon received", retraction{channel: retractChanCarbonReceived, msgType: "chat", fromFull: "bob@example.com/p"}, dm, "bob"},
		{"mam own", retraction{channel: retractChanMAM, msgType: "chat", fromFull: "me@example.com/l"}, dm, "ours"},
		{"group mms", retraction{channel: retractChanLive, msgType: "chat", fromFull: "+15550001111@cheogram.com"}, mms, "mms"},
		{"other portal", retraction{channel: retractChanLive, msgType: "chat", fromFull: "bob@example.com/p"}, dm, "other"},
		{"dm style in muc", retraction{channel: retractChanLive, msgType: "chat", fromFull: "room@muc.example.com/alice"}, muc, "alice"},
		{"muc nick", retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com/alice"}, muc, "alice"},
		{"muc other nick", retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com/mallory"}, muc, "alice"},
		{"muc occ equal", retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com/a2", occupantID: "OCC-A"}, muc, "aliceOcc"},
		{"muc occ different", retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com/alice", occupantID: "OCC-X"}, muc, "aliceOcc"},
		{"muc occ missing", retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com/alice"}, muc, "aliceOcc"},
		{"muc target without occ", retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com/alice", occupantID: "OCC-M"}, muc, "alice"},
		{"muc own other client", retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "room@muc.example.com/me"}, muc, "ourMUC"},
		{"muc in dm portal", retraction{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: "bob@example.com/alice"}, dm, "bob"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			okR, whyR := validateRetraction(tc.r, tc.p, rows[tc.target])
			okO, whyO := validateOwnership(tc.r.claim(), tc.p, rows[tc.target])
			if okR != okO || whyR != whyO {
				t.Fatalf("retraction (%v, %q) != ownership (%v, %q)", okR, whyR, okO, whyO)
			}
		})
	}
	if ok, why := validateOwnership(senderClaim{}, dm, nil); ok || why != "not_found" {
		t.Errorf("nil target = %v %q", ok, why)
	}
}

// ---- pre/post handlers with a fake environment ----

type fakeCorrEnv struct {
	byRef     map[string][]*database.Message
	kv        map[database.Key]string
	pf        portalFacts
	retracted bool
	findErr   error
	lookups   []string
	lg        zerolog.Logger
}

func newFakeCorrEnv(pf portalFacts, rows ...*database.Message) *fakeCorrEnv {
	e := &fakeCorrEnv{byRef: map[string][]*database.Message{}, kv: map[database.Key]string{}, pf: pf, lg: zerolog.Nop()}
	for _, r := range rows {
		e.byRef[string(r.ID)] = append(e.byRef[string(r.ID)], r)
		if m := targetMeta(r); m != nil {
			for _, a := range []string{m.XMPPMessageID, m.OriginID, m.StanzaID} {
				if a != "" {
					e.byRef[a] = append(e.byRef[a], r)
				}
			}
		}
	}
	return e
}

func (f *fakeCorrEnv) findByRef(_ context.Context, _ networkid.PortalKey, ref string, _ refKind) ([]*database.Message, error) {
	f.lookups = append(f.lookups, ref)
	return f.byRef[ref], f.findErr
}
func (f *fakeCorrEnv) facts(context.Context, *bridgev2.Portal) portalFacts { return f.pf }
func (f *fakeCorrEnv) kvGet(_ context.Context, k database.Key) string      { return f.kv[k] }
func (f *fakeCorrEnv) kvSet(_ context.Context, k database.Key, v string)   { f.kv[k] = v }
func (f *fakeCorrEnv) isRetracted(context.Context, networkid.PortalID, retractedSender, ...string) bool {
	return f.retracted
}
func (f *fakeCorrEnv) bodyText(_ context.Context, _ *bridgev2.Portal, c correction) string {
	return c.body
}
func (f *fakeCorrEnv) applyMentions(*bridgev2.Portal, *event.MessageEventContent, correction) {}
func (f *fakeCorrEnv) log(context.Context) *zerolog.Logger                                    { return &f.lg }

func dmFacts() portalFacts { return portalFacts{key: dmPortal(), loginID: retractTestLogin} }
func mucFacts() portalFacts {
	return portalFacts{key: mucPortal(), loginID: retractTestLogin, isMUC: true, ourOccupantFull: "room@muc.example.com/me"}
}

func dmCorrection(from, target, corrID, body string) correction {
	return correction{
		claim:    senderClaim{channel: retractChanLive, msgType: "chat", fromFull: from},
		target:   target,
		corrID:   corrID,
		body:     body,
		senderID: bareJID(from),
		ts:       time.Unix(100, 0),
	}
}

func preHandle(env *fakeCorrEnv, key networkid.PortalKey, sender bridgev2.EventSender, c correction) (*simplevent.Message[*correction], *bridgev2.EventHandlingResult) {
	evt := newCorrectionEvent(env, key, sender, c)
	return evt, evt.PreHandleWithResult(context.Background(), nil)
}

func TestCorrectionDMValid(t *testing.T) {
	orig := row(dmPortal(), "m1", "bob@example.com", &MessageMetadata{XMPPMessageID: "m1"})
	env := newFakeCorrEnv(dmFacts(), orig)
	evt, res := preHandle(env, dmPortal(), bridgev2.EventSender{Sender: "bob@example.com"}, dmCorrection("bob@example.com/p", "m1", "c1", "fixed"))
	if res != nil {
		t.Fatalf("pre-handler result = %+v, want nil (continue to the framework edit)", res)
	}
	if evt.GetTargetMessage() != "m1" {
		t.Errorf("target = %q", evt.GetTargetMessage())
	}
	if evt.GetType() != bridgev2.RemoteEventEdit || evt.ShouldCreatePortal() {
		t.Errorf("type=%v createPortal=%v", evt.GetType(), evt.ShouldCreatePortal())
	}
}

func TestCorrectionDMSenderMismatch(t *testing.T) {
	orig := row(dmPortal(), "m1", "bob@example.com", nil)
	env := newFakeCorrEnv(dmFacts(), orig)
	_, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("mallory@example.com/p", "m1", "c1", "x"))
	if res == nil || !res.Ignored {
		t.Fatalf("result = %+v, want Ignored", res)
	}
}

func TestCorrectionCarbonSentOwn(t *testing.T) {
	ours := row(dmPortal(), "m2", "me@example.com", nil)
	env := newFakeCorrEnv(dmFacts(), ours)
	c := dmCorrection("me@example.com/laptop", "m2", "c1", "fixed")
	c.claim.channel = retractChanCarbonSent
	c.senderID = "me@example.com"
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{IsFromMe: true, Sender: "me@example.com"}, c); res != nil {
		t.Fatalf("own carbon correction rejected: %+v", res)
	}
	c.claim.fromFull = "bob@example.com/p"
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, c); res == nil || !res.Ignored {
		t.Fatalf("carbon sent from someone else must be ignored: %+v", res)
	}
}

func TestCorrectionCarbonReceived(t *testing.T) {
	orig := row(dmPortal(), "m1", "bob@example.com", nil)
	env := newFakeCorrEnv(dmFacts(), orig)
	c := dmCorrection("bob@example.com/p", "m1", "c1", "fixed")
	c.claim.channel = retractChanCarbonReceived
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, c); res != nil {
		t.Fatalf("received carbon correction rejected: %+v", res)
	}
}

func mucCorrection(from, occ, target, corrID string) correction {
	return correction{
		claim:    senderClaim{channel: retractChanLive, groupchat: true, msgType: "groupchat", fromFull: from, occupantID: occ},
		target:   target,
		corrID:   corrID,
		body:     "fixed",
		senderID: from,
		ts:       time.Unix(100, 0),
	}
}

func TestCorrectionMUCOccupantIDStrict(t *testing.T) {
	orig := row(mucPortal(), "s2", "room@muc.example.com/alice", &MessageMetadata{XMPPMessageID: "s2", OccupantID: "OCC-A"})
	env := newFakeCorrEnv(mucFacts(), orig)
	// Same nick, different occupant-id: a nick can be reused by someone else.
	if _, res := preHandle(env, mucPortal(), bridgev2.EventSender{}, mucCorrection("room@muc.example.com/alice", "OCC-EVIL", "s2", "c1")); res == nil || !res.Ignored {
		t.Fatalf("different occupant-id must be ignored: %+v", res)
	}
	// Different nick (renamed), equal occupant-id: valid.
	if _, res := preHandle(env, mucPortal(), bridgev2.EventSender{}, mucCorrection("room@muc.example.com/alice2", "OCC-A", "s2", "c2")); res != nil {
		t.Fatalf("equal occupant-id must be valid: %+v", res)
	}
}

func TestCorrectionMUCTargetWithoutOccupantID(t *testing.T) {
	orig := row(mucPortal(), "s1", "room@muc.example.com/alice", &MessageMetadata{XMPPMessageID: "s1"})
	env := newFakeCorrEnv(mucFacts(), orig)
	if _, res := preHandle(env, mucPortal(), bridgev2.EventSender{}, mucCorrection("room@muc.example.com/alice", "OCC-M", "s1", "c1")); res == nil || !res.Ignored {
		t.Fatalf("occupant-id on the correction but none on the target must be ignored: %+v", res)
	}
	if _, res := preHandle(env, mucPortal(), bridgev2.EventSender{}, mucCorrection("room@muc.example.com/alice", "", "s1", "c2")); res != nil {
		t.Fatalf("nick match without occupant-ids must be valid: %+v", res)
	}
}

func TestCorrectionMUCOwnOccupant(t *testing.T) {
	ours := row(mucPortal(), "matrix:$e", "me@example.com", &MessageMetadata{XMPPMessageID: "o1"})
	env := newFakeCorrEnv(mucFacts(), ours)
	c := mucCorrection("room@muc.example.com/me", "", "o1", "c1")
	c.senderID = "me@example.com"
	if _, res := preHandle(env, mucPortal(), bridgev2.EventSender{IsFromMe: true, Sender: "me@example.com"}, c); res != nil {
		t.Fatalf("correction from our own occupant rejected: %+v", res)
	}
	c.claim.fromFull = "room@muc.example.com/alice"
	if _, res := preHandle(env, mucPortal(), bridgev2.EventSender{}, c); res == nil || !res.Ignored {
		t.Fatalf("someone else's nick on our message must be ignored: %+v", res)
	}
}

func TestCorrectionGroupMMSRejected(t *testing.T) {
	key := networkid.PortalKey{ID: "+15550001111,+15550002222@cheogram.com", Receiver: retractTestLogin}
	orig := row(key, "g1", "+15550001111@cheogram.com", &MessageMetadata{XMPPMessageID: "g1"})
	env := newFakeCorrEnv(portalFacts{key: key, loginID: retractTestLogin, isGroupMMS: true}, orig)
	c := dmCorrection("+15550001111@cheogram.com", "g1", "c1", "fixed")
	if _, res := preHandle(env, key, bridgev2.EventSender{}, c); res == nil || !res.Ignored {
		t.Fatalf("group MMS correction must be ignored: %+v", res)
	}
}

func TestCorrectionWrongPortal(t *testing.T) {
	foreign := row(networkid.PortalKey{ID: "other@example.com", Receiver: retractTestLogin}, "m1", "bob@example.com", &MessageMetadata{XMPPMessageID: "m1"})
	env := newFakeCorrEnv(dmFacts(), foreign)
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("bob@example.com/p", "m1", "c1", "x")); res == nil || !res.Ignored {
		t.Fatalf("a message of another portal must never be edited: %+v", res)
	}
}

func TestCorrectionUnknownTargetIgnored(t *testing.T) {
	env := newFakeCorrEnv(dmFacts())
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("bob@example.com/p", "nope", "c1", "x")); res == nil || !res.Ignored || res.Error != nil {
		t.Fatalf("unknown target: %+v", res)
	}
	env.findErr = errors.New("db down")
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("bob@example.com/p", "nope", "c1", "x")); res == nil || res.Error == nil {
		t.Fatalf("lookup error must fail: %+v", res)
	}
}

// An older client corrects its previous correction: <replace id> names the correction's id, which the
// alias record maps back to the original.
func TestCorrectionChainViaAlias(t *testing.T) {
	orig := row(dmPortal(), "m1", "bob@example.com", &MessageMetadata{XMPPMessageID: "m1"})
	env := newFakeCorrEnv(dmFacts(), orig)

	first := dmCorrection("bob@example.com/p", "m1", "c1", "fixed once")
	evt, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, first)
	if res != nil {
		t.Fatalf("first: %+v", res)
	}
	// Run the convert + post handlers the way bridgev2 does (existing = the original's rows).
	if _, err := evt.ConvertEdit(context.Background(), nil, nil, []*database.Message{orig}); err != nil {
		t.Fatal(err)
	}
	evt.PostHandle(context.Background(), nil)
	key := correctedKey(retractTestLogin, "bob@example.com", "snd=bob%40example.com", "c1")
	if env.kv[key] != "m1" {
		t.Fatalf("alias not recorded: %v", env.kv)
	}

	second := dmCorrection("bob@example.com/p", "c1", "c2", "fixed twice")
	evt2, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, second)
	if res != nil {
		t.Fatalf("chained correction rejected: %+v", res)
	}
	if evt2.GetTargetMessage() != "m1" {
		t.Errorf("chained target = %q, want the original m1", evt2.GetTargetMessage())
	}
	// Another sender cannot use bob's alias.
	third := dmCorrection("mallory@example.com/p", "c1", "c3", "x")
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, third); res == nil || !res.Ignored {
		t.Fatalf("alias must be sender-scoped: %+v", res)
	}
}

func TestCorrectionReplayDeduped(t *testing.T) {
	orig := row(dmPortal(), "m1", "bob@example.com", &MessageMetadata{XMPPMessageID: "m1"})
	env := newFakeCorrEnv(dmFacts(), orig)
	env.kv[correctedKey(retractTestLogin, "bob@example.com", "snd=bob%40example.com", "c1")] = "m1"
	_, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("bob@example.com/p", "m1", "c1", "again"))
	if res == nil || !res.Ignored {
		t.Fatalf("replayed correction must be ignored: %+v", res)
	}
	if len(env.lookups) != 0 {
		t.Errorf("dedupe must run before any message lookup, looked up %v", env.lookups)
	}
}

func TestCorrectionOfRetractedIgnored(t *testing.T) {
	orig := row(dmPortal(), "m1", "bob@example.com", &MessageMetadata{XMPPMessageID: "m1"})
	env := newFakeCorrEnv(dmFacts(), orig)
	env.retracted = true
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("bob@example.com/p", "m1", "c1", "x")); res == nil || !res.Ignored {
		t.Fatalf("correction of a retracted message must be ignored: %+v", res)
	}
}

func TestCorrectionMediaIgnored(t *testing.T) {
	media := row(dmPortal(), "m1", "bob@example.com", &MessageMetadata{XMPPMessageID: "m1", Media: true})
	env := newFakeCorrEnv(dmFacts(), media)
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("bob@example.com/p", "m1", "c1", "new caption")); res == nil || !res.Ignored {
		t.Fatalf("correction of a media row must be ignored: %+v", res)
	}
	text := row(dmPortal(), "m2", "bob@example.com", &MessageMetadata{XMPPMessageID: "m2"})
	env = newFakeCorrEnv(dmFacts(), text)
	c := dmCorrection("bob@example.com/p", "m2", "c2", "https://example.com/x.png")
	c.hasOOB = true
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, c); res == nil || !res.Ignored {
		t.Fatalf("correction carrying an OOB url must be ignored: %+v", res)
	}
}

func TestCorrectionWithoutBodyIgnored(t *testing.T) {
	orig := row(dmPortal(), "m1", "bob@example.com", &MessageMetadata{XMPPMessageID: "m1"})
	env := newFakeCorrEnv(dmFacts(), orig)
	if _, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("bob@example.com/p", "m1", "c1", "  ")); res == nil || !res.Ignored {
		t.Fatalf("empty body: %+v", res)
	}
	// And on the live path nothing is queued for a body-less correction (not an edit, not a message).
	x, got := hookClient()
	runHandler(t, x, `<message type="chat" from="bob@example.com/p" id="c1"><replace xmlns="urn:xmpp:message-correct:0" id="m1"/></message>`)
	if len(*got) != 0 {
		t.Errorf("body-less correction queued %d events", len(*got))
	}
}

func TestCorrectionSetsForceEditOrigSender(t *testing.T) {
	orig := row(dmPortal(), "matrix:$e", "me@example.com", &MessageMetadata{XMPPMessageID: "o1"})
	env := newFakeCorrEnv(dmFacts(), orig)
	c := dmCorrection("me@example.com/laptop", "o1", "c1", "fixed")
	c.claim.channel = retractChanCarbonSent
	evt := newCorrectionEvent(env, dmPortal(), bridgev2.EventSender{IsFromMe: true, Sender: "me@example.com"}, c)
	if evt.GetSender().ForceEditOrigSender {
		t.Fatal("must not be set before validation")
	}
	if res := evt.PreHandleWithResult(context.Background(), nil); res != nil {
		t.Fatalf("result = %+v", res)
	}
	if !evt.GetSender().ForceEditOrigSender || evt.GetTargetMessage() != "matrix:$e" {
		t.Errorf("sender=%+v target=%q", evt.GetSender(), evt.GetTargetMessage())
	}
}

func TestConvertCorrectionBuildsTextEdit(t *testing.T) {
	orig := row(dmPortal(), "m1", "bob@example.com", &MessageMetadata{XMPPMessageID: "m1"})
	env := newFakeCorrEnv(dmFacts(), orig)
	evt, res := preHandle(env, dmPortal(), bridgev2.EventSender{}, dmCorrection("bob@example.com/p", "m1", "c1", "fixed text"))
	if res != nil {
		t.Fatal(res)
	}
	ce, err := evt.ConvertEdit(context.Background(), nil, nil, []*database.Message{orig})
	if err != nil || len(ce.ModifiedParts) != 1 {
		t.Fatalf("convert: %v %+v", err, ce)
	}
	p := ce.ModifiedParts[0]
	if p.Part != orig || p.Type != event.EventMessage || p.Content.MsgType != event.MsgText || p.Content.Body != "fixed text" {
		t.Errorf("part = %+v content=%+v", p, p.Content)
	}
}

// ---- inbound paths ----

func TestInboundCorrectionLiveDMQueuesEdit(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="chat" from="Bob@example.com/p" id="c1"><body>fixed</body><replace xmlns="urn:xmpp:message-correct:0" id="m1"/></message>`)
	if len(*got) != 1 {
		t.Fatalf("queued %d events, want exactly one", len(*got))
	}
	evt, ok := (*got)[0].(*simplevent.Message[*correction])
	if !ok {
		t.Fatalf("queued %T, want an edit, never a message", (*got)[0])
	}
	if evt.GetType() != bridgev2.RemoteEventEdit || evt.GetPortalKey().ID != "Bob@example.com" || evt.ShouldCreatePortal() {
		t.Errorf("type=%v portal=%v create=%v", evt.GetType(), evt.GetPortalKey(), evt.ShouldCreatePortal())
	}
	c := evt.Data
	if c.target != "m1" || c.corrID != "c1" || c.body != "fixed" || c.claim.channel != retractChanLive || c.senderID != "Bob@example.com" {
		t.Errorf("correction = %+v", c)
	}
}

func TestInboundCorrectionMUCEchoDropped(t *testing.T) {
	x, got := hookClient()
	x.mucOurOccupantJID = map[string]string{"room@muc.example.com": "room@muc.example.com/me"}
	x.mucEcho.register("room@muc.example.com", "sent1")
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="sent1"><body>fixed</body><origin-id xmlns="urn:xmpp:sid:0" id="sent1"/><replace xmlns="urn:xmpp:message-correct:0" id="o1"/></message>`)
	if len(*got) != 0 {
		t.Fatalf("the echo of our own correction must not be bridged: %d events", len(*got))
	}
	// The same stanza from another client of ours (not registered) is a correction by us.
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/me" id="other1"><body>fixed</body><replace xmlns="urn:xmpp:message-correct:0" id="o1"/></message>`)
	if len(*got) != 1 {
		t.Fatalf("queued %d events", len(*got))
	}
	evt := (*got)[0].(*simplevent.Message[*correction])
	if !evt.GetSender().IsFromMe || evt.Data.senderID != "me@example.com" || !evt.Data.claim.groupchat {
		t.Errorf("sender=%+v data=%+v", evt.GetSender(), evt.Data)
	}
}

func TestInboundCorrectionMUCGhost(t *testing.T) {
	x, got := hookClient()
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="c1"><body>fixed</body><occupant-id xmlns="urn:xmpp:occupant-id:0" id="OCC-B"/><replace xmlns="urn:xmpp:message-correct:0" id="m1"/></message>`)
	if len(*got) != 1 {
		t.Fatalf("queued %d events", len(*got))
	}
	c := (*got)[0].(*simplevent.Message[*correction]).Data
	if c.claim.occupantID != "OCC-B" || c.senderID != "room@muc.example.com/bob" {
		t.Errorf("correction = %+v", c)
	}
}

func TestInboundCorrectionCarbon(t *testing.T) {
	const fmtCarbon = `<message from="me@example.com"><%[1]s xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="%[2]s" to="%[3]s" type="chat" id="c1"><body>fixed</body><replace xmlns="urn:xmpp:message-correct:0" id="m1"/></message></forwarded></%[1]s></message>`
	for _, tc := range []struct {
		kind, from, to, channel string
		sender                  bridgev2.EventSender
	}{
		{"sent", "me@example.com/laptop", "Bob@example.com", retractChanCarbonSent, bridgev2.EventSender{IsFromMe: true, SenderLogin: "me@example.com", Sender: "me@example.com"}},
		{"received", "Bob@example.com/p", "me@example.com", retractChanCarbonReceived, bridgev2.EventSender{SenderLogin: "Bob@example.com", Sender: "Bob@example.com"}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			x, got := hookClient()
			runHandler(t, x, fmt.Sprintf(fmtCarbon, tc.kind, tc.from, tc.to))
			if len(*got) != 1 {
				t.Fatalf("queued %d events", len(*got))
			}
			evt, ok := (*got)[0].(*simplevent.Message[*correction])
			if !ok {
				t.Fatalf("queued %T", (*got)[0])
			}
			if evt.GetPortalKey().ID != "bob@example.com" || evt.Data.claim.channel != tc.channel || evt.GetSender() != tc.sender {
				t.Errorf("portal=%v data=%+v sender=%+v", evt.GetPortalKey(), evt.Data, evt.GetSender())
			}
		})
	}
	// A forged carbon (outer from is not our account) never reaches the correction path.
	x, got := hookClient()
	runHandler(t, x, `<message from="evil@example.com/x" to="me@example.com" type="chat"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/phone" to="bob@example.com" id="c1"><body>forged</body><replace xmlns="urn:xmpp:message-correct:0" id="m1"/></message></forwarded></sent></message>`)
	if len(*got) != 0 {
		t.Errorf("forged correction queued: %v", *got)
	}
}

func corrItem(id, from, replace, body string, ts int64) mamResultItem {
	it := mamItem(id, from, "")
	it.Body = body
	it.Timestamp = time.Unix(1_700_000_000+ts, 0)
	it.Extras.Replace = replace
	return it
}

func TestMAMCatchUpCorrectionQueuesEdit(t *testing.T) {
	x, got := hookClient()
	ctx := context.Background()
	pk := networkid.PortalKey{ID: "bob@example.com", Receiver: retractTestLogin}
	if x.queueMAMCorrection(ctx, pk, mamItem("m1", "bob@example.com/p", "")) || len(*got) != 0 {
		t.Fatal("a normal item is not a correction")
	}
	if !x.queueMAMCorrection(ctx, pk, corrItem("c1", "bob@example.com/p", "m1", "fixed", 5)) || len(*got) != 1 {
		t.Fatal("correction item must queue one event")
	}
	evt, ok := (*got)[0].(*simplevent.Message[*correction])
	if !ok {
		t.Fatalf("queued %T", (*got)[0])
	}
	c := evt.Data
	if evt.GetPortalKey() != pk || c.claim.channel != retractChanMAM || c.corrID != "c1" || c.target != "m1" || c.senderID != "bob@example.com" || evt.GetTimestamp().Unix() != 1_700_000_005 {
		t.Errorf("event = %+v data = %+v", evt, c)
	}
	// A correction without a body is consumed (never bridged as a message) without an event.
	if !x.queueMAMCorrection(ctx, pk, corrItem("c2", "bob@example.com/p", "m1", "", 6)) || len(*got) != 1 {
		t.Error("body-less correction must be consumed without an event")
	}
	if mamItemBackfillable(corrItem("c3", "bob@example.com/p", "m1", "x", 7), nil) {
		t.Error("a correction must never be backfilled as a message")
	}
}

func TestFoldBatchCorrections(t *testing.T) {
	items := []mamResultItem{
		corrItem("c2", "BOB@example.com/p2", "c1", "third", 30), // chained: replaces the first correction's id
		corrItem("m1", "bob@example.com/p", "", "first", 10),
		corrItem("c1", "bob@example.com/p", "m1", "second", 20),
		corrItem("m2", "bob@example.com/p", "", "untouched", 11),
		corrItem("c3", "mallory@example.com/p", "m2", "hijack", 40), // other sender: ignored
		corrItem("c4", "bob@example.com/p", "gone", "orphan", 41),   // original not in the batch: dropped
		corrItem("m3", "me@example.com/l", "", "mine", 12),
		corrItem("c5", "me@example.com/l", "m3", "mine fixed", 50),
		corrItem("c6", "me@example.com/l", "m3", "mine fixed older", 45), // newest by timestamp wins
	}
	got := foldBatchCorrections(items)
	bodies := map[string]string{}
	var order []string
	for _, it := range got {
		bodies[it.StanzaID] = it.Body
		order = append(order, it.StanzaID)
	}
	if !reflect.DeepEqual(order, []string{"m1", "m2", "m3"}) {
		t.Fatalf("order = %v, want the originals in input order and no correction", order)
	}
	if bodies["m1"] != "third" || bodies["m2"] != "untouched" || bodies["m3"] != "mine fixed" {
		t.Errorf("bodies = %v", bodies)
	}
	if items[1].Body != "first" {
		t.Error("input slice must not be modified")
	}
	if same := foldBatchCorrections(items[1:2]); len(same) != 1 {
		t.Error("batches without corrections pass through")
	}
}

func TestMAMCorrectionIDFallsBackToOriginID(t *testing.T) {
	it := corrItem("xmp:20231114", "bob@example.com/p", "m1", "x", 1)
	it.OriginID = "origin-9"
	if got := mamCorrectionID(it); got != "origin-9" {
		t.Errorf("generated id must yield the origin-id, got %q", got)
	}
	it.StanzaID = "real-id"
	if got := mamCorrectionID(it); got != "real-id" {
		t.Errorf("a real id wins like on the live path, got %q", got)
	}
	it.StanzaID, it.OriginID = "xmp:1", ""
	if got := mamCorrectionID(it); got != "" {
		t.Errorf("no stable id: got %q", got)
	}
	x, got := hookClient()
	it.StanzaID, it.OriginID = "xmp:1", "origin-9"
	pk := networkid.PortalKey{ID: "bob@example.com", Receiver: retractTestLogin}
	x.queueMAMCorrection(context.Background(), pk, it)
	evt := (*got)[0].(*simplevent.Message[*correction])
	if evt.Data.corrID != "origin-9" {
		t.Errorf("queued corrID = %q", evt.Data.corrID)
	}
}

func TestFoldBatchCorrectionsChainWithoutTimestamps(t *testing.T) {
	// Equal timestamps, the chained correction (c2 replaces c1) listed first.
	items := []mamResultItem{
		corrItem("c2", "bob@example.com/p", "c1", "third", 0),
		corrItem("m1", "bob@example.com/p", "", "first", 0),
		corrItem("c1", "bob@example.com/p", "m1", "second", 0),
	}
	got, folded := foldBatchCorrectionsDetailed(items)
	if len(got) != 1 || got[0].Body != "third" {
		t.Fatalf("got %+v, want the chain end to win", got)
	}
	if len(folded) != 2 || folded[0].corr.StanzaID != "c1" || folded[1].corr.StanzaID != "c2" {
		t.Fatalf("folded = %+v, want chain order c1, c2", folded)
	}
}

func TestFoldBatchCorrectionsRecordsKeys(t *testing.T) {
	m1 := corrItem("m1", "bob@example.com/p", "", "first", 1)
	c1 := corrItem("xmp:gen", "BOB@example.com/p", "m1", "second", 2)
	c1.OriginID = "o1"
	_, folded := foldBatchCorrectionsDetailed([]mamResultItem{m1, c1})
	keys := foldedCorrectionKeys(retractTestLogin, "bob@example.com", folded)
	want := correctedKey(retractTestLogin, "bob@example.com", senderScopes("bob@example.com", "")[0], "o1")
	if keys[want] != "m1" || len(keys) != 1 {
		t.Fatalf("keys = %v, want %q -> m1", keys, want)
	}
	// The same key the live path writes for this correction (id firstNonEmpty(id, origin-id)).
	live := correction{senderID: "bob@example.com", corrID: "o1"}
	if correctedKey(retractTestLogin, "bob@example.com", senderScopes(live.senderID, live.occupantID())[0], live.corrID) != want {
		t.Error("fold key differs from the live key")
	}
}

func TestFoldBatchCorrectionsSkipsGroupMMS(t *testing.T) {
	const group = "+15550000001,+15550000002@cheogram.com"
	items := []mamResultItem{
		corrItem("m1", group+"/+15550000001", "", "first", 1),
		corrItem("c1", group+"/+15550000001", "m1", "second", 2),
	}
	got, folded := foldBatchCorrectionsDetailed(items)
	if len(folded) != 0 || len(got) != 1 || got[0].Body != "first" {
		t.Fatalf("group MMS correction folded: got=%+v folded=%+v", got, folded)
	}
}

// ---- logs ----

func TestCorrectionContentNotLogged(t *testing.T) {
	const secret = "CORRECTEDSECRET"
	x, got := hookClient()
	x.conn = &JabberConnector{}
	buf := &bytes.Buffer{}
	x.log = zerolog.New(buf).Level(zerolog.TraceLevel)
	x.mucOurOccupantJID = map[string]string{selfEchoRoom: selfEchoRoom + "/me"}

	runHandler(t, x, `<message type="chat" from="bob@example.com/res" id="c1"><body>`+secret+` dm</body><replace xmlns="urn:xmpp:message-correct:0" id="m1"/></message>`)
	runHandler(t, x, `<message type="groupchat" from="room@muc.example.com/bob" id="c2"><body>`+secret+` muc</body><replace xmlns="urn:xmpp:message-correct:0" id="m2"/></message>`)
	runHandler(t, x, `<message from="me@example.com"><sent xmlns="urn:xmpp:carbons:2"><forwarded xmlns="urn:xmpp:forward:0"><message xmlns="jabber:client" from="me@example.com/l" to="bob@example.com" type="chat" id="c3"><body>`+secret+` carbon</body><replace xmlns="urn:xmpp:message-correct:0" id="m3"/></message></forwarded></sent></message>`)
	if len(*got) != 3 {
		t.Fatalf("queued %d events", len(*got))
	}
	// Run the handlers of every queued event too (unknown target, valid target, replay).
	lg := zerolog.New(buf).Level(zerolog.TraceLevel)
	for i, e := range *got {
		evt := e.(*simplevent.Message[*correction])
		orig := row(evt.GetPortalKey(), "m1", evt.Data.senderID, &MessageMetadata{XMPPMessageID: evt.Data.target})
		env := newFakeCorrEnv(portalFacts{key: evt.GetPortalKey(), loginID: retractTestLogin, isMUC: evt.Data.claim.groupchat, ourOccupantFull: selfEchoRoom + "/me"})
		env.lg = lg
		if i == 1 {
			env.byRef[evt.Data.target] = []*database.Message{orig}
		}
		fresh := newCorrectionEvent(env, evt.GetPortalKey(), evt.GetSender(), *evt.Data)
		fresh.PreHandleWithResult(context.Background(), nil)
		fresh.PostHandle(context.Background(), nil)
	}
	out := buf.String()
	if out == "" {
		t.Fatal("nothing was logged; the test proves nothing")
	}
	if strings.Contains(out, secret) {
		t.Errorf("logs leak corrected text:\n%s", out)
	}
}
