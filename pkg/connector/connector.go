package connector

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"mellium.im/xmpp/jid"

	"github.com/williamkray/jmpchat-matrix-bridge/pkg/telemetry"
)

// JabberConfig is the connector-specific config under network.jabber (framework nests it by network ID).
type JabberConfig struct {
	ResourcePrefix string        `yaml:"resource_prefix"`
	Cheogram       bool          `yaml:"cheogram"` // resolve phone numbers and short codes to Cheogram JIDs (SMS/MMS)
	CardDAV        CardDAVConfig `yaml:"carddav"`
	// AllowPrivateServerOverride lets a per-login server override (login password-server) point at a
	// private, loopback or link-local address. Off by default (SSRF guard).
	AllowPrivateServerOverride bool `yaml:"allow_private_server_override"`
	// Chat signals (XEP-0085 typing, XEP-0333 markers, XEP-0184 receipts). Pointers so that a missing key
	// means the default (true); see signalPrefs.
	SendTyping           *bool `yaml:"send_typing"`
	SendReadMarkers      *bool `yaml:"send_read_markers"`
	SendDeliveryReceipts *bool `yaml:"send_delivery_receipts"`
	MUCReadMarkers       *bool `yaml:"muc_read_markers"`
	// MUCNickSync keeps the per-room Matrix display name and the MUC nick in sync (muc_nick_sync.go).
	// Pointer so that a missing key means the default (true).
	MUCNickSync *bool `yaml:"muc_nick_sync"`
}

// allowPrivateServerOverride is nil-safe (tests and stub paths have no config).
func (c *JabberConnector) allowPrivateServerOverride() bool {
	return c != nil && c.networkConfig != nil && c.networkConfig.AllowPrivateServerOverride
}

// dialOptions returns the dial options of this login: the stored override plus the SSRF setting.
func (x *xmppClient) dialOptions() dialOptions {
	opts := x.meta.dialOptions()
	opts.AllowPrivate = x.conn.allowPrivateServerOverride()
	return opts
}

// BackfillConfig holds backfill settings from config.backfill, used by MAM backfill-on-view and full backfill on login / sync-roster.
type BackfillConfig struct {
	Enabled            bool `yaml:"enabled"`
	MaxInitialMessages int  `yaml:"max_initial_messages"`
}

// JabberConnector implements the bridgev2 NetworkConnector interface for
// the Matrix-Jabber (XMPP) bridge.
type JabberConnector struct {
	br               *bridgev2.Bridge
	networkConfig    *JabberConfig   // set from main after LoadConfig; bridge only has BridgeConfig
	backfillConfig   *BackfillConfig // set from main from config.backfill
	deliveryReceipts bool            // whether to send read receipts when messages are bridged (matrix.delivery_receipts)
	appServiceID     string          // appservice.id from config, used in ghost display names (e.g. "user@example.com (cheogram)")

	extnOnce sync.Once
	extn     connectorExtension // optional build-tagged extension (nil in the default build)
}

// Ensure we implement the interface at compile time.
var _ bridgev2.NetworkConnector = (*JabberConnector)(nil)

// RosterSyncer is implemented by the real XMPP client (not the stub). Use it to force a
// roster fetch so display names and room names pick up renames done in another client.
type RosterSyncer interface {
	SyncRoster(ctx context.Context)
}

// FullBackfillRunner is implemented by the real XMPP client. RunFullBackfill runs MAM backfill
// for all roster DMs (skips MUC), so portals are created and history filled before new messages arrive.
// When backfill is enabled, this is triggered on sync-roster and optionally after login (full_backfill_on_login).
type FullBackfillRunner interface {
	RunFullBackfill(ctx context.Context)
}

// RosterManager is implemented by the real XMPP client. Use it to add or rename contacts
// in the Jabber roster from Matrix (e.g. !jabber add-to-roster <jid> <name>).
type RosterManager interface {
	SetRosterItem(ctx context.Context, jidStr, displayName string) error
}

// RosterRemover is implemented by the real XMPP client. Use it to remove a contact from the
// Jabber roster from Matrix (e.g. !jabber remove-from-roster <jid>).
type RosterRemover interface {
	RemoveRosterItem(ctx context.Context, jidStr string) error
}

// RosterSearcher is implemented by the real XMPP client. Use it to search the roster
// for contacts matching a JID or name pattern.
type RosterSearcher interface {
	SearchRoster(ctx context.Context, query string) ([]RosterContact, error)
}

// RosterContact represents a contact in the roster.
type RosterContact struct {
	JID  string
	Name string
}

// MUCJoiner is implemented by the real XMPP client. Use it to join or leave MUC rooms
// from bridge commands (e.g. !jabber join, !jabber leave).
type MUCJoiner interface {
	JoinMUC(ctx context.Context, roomJID string) error
	LeaveMUC(ctx context.Context, roomJID string) error
}

// MUCNickStatus is the immediate outcome of MUCNickChanger.SetMUCNick.
type MUCNickStatus int

const (
	MUCNickSaved     MUCNickStatus = iota // not in the room: stored, used at the next join
	MUCNickUnchanged                      // already the nick in the room: stored as the preferred nick
	MUCNickChanging                       // nick-change presence sent; the result follows as a notice
)

// MUCNickChanger is implemented by the real XMPP client (muc-nick command).
type MUCNickChanger interface {
	SetMUCNick(ctx context.Context, roomJID, nick string) (status MUCNickStatus, current string, err error)
}

// MUCJoinStatus is the immediate outcome of the join command.
type MUCJoinStatus int

const (
	MUCJoinSent      MUCJoinStatus = iota // join presence sent
	MUCJoinAlreadyIn                      // already in the room under the chosen nick: nothing sent
	MUCJoinRenaming                       // already in the room under another nick: nick change sent
)

// MUCJoinReporter is implemented by the real XMPP client (join command).
type MUCJoinReporter interface {
	JoinMUCStatus(ctx context.Context, roomJID string) (status MUCJoinStatus, current, want string, err error)
}

// onceWarnDebugIdentifiers ensures we only log the debug-identifiers warning once per process.
var onceWarnDebugIdentifiers sync.Once

// warnIfDebugIdentifiers logs a one-time warning that debug log output may contain
// JIDs and other potentially private data. Call before emitting Debug-level logs
// that include identifiers, so operators know to protect log output when using debug.
func warnIfDebugIdentifiers(log *zerolog.Logger) {
	onceWarnDebugIdentifiers.Do(func() {
		log.Warn().Msg("Debug logging may include JIDs and other potentially private data; ensure log output is protected")
	})
}

// Init stores the bridge reference.
func (c *JabberConnector) Init(br *bridgev2.Bridge) {
	c.br = br
	if e := c.extension(); e != nil {
		e.init(br)
	}
}

// SetNetworkConfig sets the decoded network section config. Called from main after LoadConfig
// because the bridge only receives BridgeConfig, not the full config with Network.
func (c *JabberConnector) SetNetworkConfig(cfg *JabberConfig) {
	c.networkConfig = cfg
}

// SetDeliveryReceipts sets whether the bridge should send read receipts after successfully
// bridging a message. Called from main from matrix.delivery_receipts in the full config.
func (c *JabberConnector) SetDeliveryReceipts(enabled bool) {
	c.deliveryReceipts = enabled
}

// SetBackfillConfig sets backfill settings from config.backfill. Called from main after LoadConfig.
func (c *JabberConnector) SetBackfillConfig(cfg *BackfillConfig) {
	c.backfillConfig = cfg
}

// SetAppServiceID sets the appservice ID from config. Used in ghost display names (e.g. "user@example.com (cheogram)").
func (c *JabberConnector) SetAppServiceID(id string) {
	c.appServiceID = id
}

const (
	cheogramEnvVar       = "JMPCHAT_BRIDGE_CHEOGRAM"
	cheogramEnvVarLegacy = "MAUTRIX_JABBER_CHEOGRAM" // pre-rename name, still honoured
)

// cheogramEnv reports the env fallback: JMPCHAT_BRIDGE_CHEOGRAM wins when set to a
// parseable bool (even false); otherwise the legacy MAUTRIX_JABBER_CHEOGRAM is used.
func cheogramEnv() bool {
	for _, name := range []string{cheogramEnvVar, cheogramEnvVarLegacy} {
		v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
		if v == "" {
			continue
		}
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return false
}

// CheogramEnabled returns true if network.cheogram is true in the config,
// or if JMPCHAT_BRIDGE_CHEOGRAM (legacy MAUTRIX_JABBER_CHEOGRAM) is true/1 (fallback when config is unavailable).
func CheogramEnabled(br *bridgev2.Bridge) bool {
	if br != nil {
		if cfg := getJabberConfigFromBridge(br); cfg != nil && cfg.Cheogram {
			return true
		}
	}
	return cheogramEnv()
}

// getJabberConfigFromBridge returns the connector's network config. The bridge's Config is only
// BridgeConfig (no Network field); main decodes the network section and sets it via SetNetworkConfig.
func getJabberConfigFromBridge(br *bridgev2.Bridge) *JabberConfig {
	if br == nil {
		return nil
	}
	v := reflect.ValueOf(br).Elem()
	netField := v.FieldByName("Network")
	if !netField.IsValid() || netField.IsNil() {
		return nil
	}
	if jc, ok := netField.Interface().(*JabberConnector); ok && jc != nil {
		return jc.networkConfig
	}
	return nil
}

// Start is called when the bridge is starting. In the scaffold it is a no-op;
// later it will load UserLogins and spawn XMPP reconnect loops.
func (c *JabberConnector) Start(ctx context.Context) error {
	if e := c.extension(); e != nil {
		return e.start(ctx)
	}
	return nil
}

// GetName returns bridge metadata for m.bridge and example config.
func (c *JabberConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:          "Jabber",
		NetworkURL:           "https://xmpp.org/",
		NetworkID:            "jabber",
		BeeperBridgeType:     "github.com/williamkray/jmpchat-matrix-bridge",
		DefaultPort:          29318, // mau.fi/ports or any free port
		DefaultCommandPrefix: "!jabber",
	}
}

// GetDBMetaTypes is now in dbmeta.go to keep metadata types organized.

// GetCapabilities returns general network capabilities. ImplicitReadReceipts is true so that
// when matrix.delivery_receipts is enabled, the bridge sends a read receipt when a message
// is successfully bridged to Jabber. Actual sending is gated by deliveryReceipts in HandleMatrixMessage.
func (c *JabberConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{
		ImplicitReadReceipts: true,
	}
}

// GetConfig returns the example config snippet for network, the struct to unmarshal into, and an upgrader.
// The upgrader must Copy our keys (e.g. cheogram) from the user's config into the base during upgrade,
// otherwise the framework writes the example value (false) and overwrites the user's true on restart.
func (c *JabberConnector) GetConfig() (example string, data any, upgrader configupgrade.Upgrader) {
	example = `# Connector config (under network by the framework).
# The XMPP server is found at login by DNS SRV lookup (xmpps-client, xmpp-client) on the JID's domain,
# falling back to the domain on port 5222. A per-login override is set with "login password-server".
# resource_prefix: "mautrix"
# Resolve phone numbers and short codes to Cheogram JIDs (SMS/MMS). Set to true to enable.
cheogram: false
# Allow a per-login server override (login password-server), and DNS SRV targets on non-standard
# ports (other than 5222/5223), to point at private, loopback or link-local addresses. Leave false
# unless your XMPP server runs on your own network.
allow_private_server_override: false
# Chat signals sent to XMPP contacts. All default to true; set to false to keep them private.
# Typing (XEP-0085): tell a contact when you are typing in Matrix. Only sent to contacts whose client
# showed chat state support this session, never to Cheogram (SMS/MMS) numbers.
send_typing: true
# Read markers (XEP-0333) in one-to-one chats: tell the contact which of their messages you read.
# Never sent to Cheogram (SMS/MMS) numbers.
send_read_markers: true
# Delivery receipts (XEP-0184): confirm to the sender that a message that asked for it reached the bridge.
send_delivery_receipts: true
# Read markers in group chats (MUCs): tells every occupant of the room which message you read (the same
# as the one-to-one setting, but visible to the whole room). Set to false if you do not want that.
muc_read_markers: true
# Keep your nick in XMPP group chats (MUCs) and your display name in the bridged Matrix room in sync,
# both ways. Setting a display name for one bridged room (for example /myroomnick) changes your nick
# in that XMPP room; a nick change on XMPP (another client, a taken nick) updates your display name in
# that Matrix room (needs double puppeting). Your global Matrix display name is never sent to XMPP.
muc_nick_sync: true
`
	e := c.extension()
	if e != nil {
		example += e.exampleConfig()
	}
	example += cardDAVExampleConfig
	upgrader = configupgrade.SimpleUpgrader(func(helper configupgrade.Helper) {
		// Copy user's cheogram value into base so config save preserves it (framework adds "network" prefix).
		helper.Copy(configupgrade.Bool, "cheogram")
		helper.Copy(configupgrade.Bool, "allow_private_server_override")
		helper.Copy(configupgrade.Bool, "send_typing")
		helper.Copy(configupgrade.Bool, "send_read_markers")
		helper.Copy(configupgrade.Bool, "send_delivery_receipts")
		helper.Copy(configupgrade.Bool, "muc_read_markers")
		helper.Copy(configupgrade.Bool, "muc_nick_sync")
		upgradeCardDAVConfig(helper)
		if e != nil {
			e.upgradeConfig(helper)
		}
	})
	return example, &JabberConfig{}, upgrader
}

// LoadUserLogin is called when a UserLogin is loaded (e.g. after login or on bridge start).
// It must set login.Client to a NetworkAPI. If metadata has credentials, we connect to XMPP.
func (c *JabberConnector) LoadUserLogin(ctx context.Context, login *bridgev2.UserLogin) error {
	meta, _ := login.Metadata.(*JabberLoginMeta)
	if meta == nil || meta.Password == "" {
		login.Client = &stubJabberClient{loginID: login.ID}
		return nil
	}
	client := &xmppClient{loginID: login.ID, meta: meta, log: login.Log, login: login, conn: c}
	login.Client = client
	if e := c.extension(); e != nil {
		e.loginLoaded(login, client)
	}
	// Start a background reconnect loop so the XMPP session is kept alive.
	// Use a cancellable context so LogoutRemote can stop the loop and prevent reconnecting after logout.
	bgCtx, cancel := context.WithCancel(login.Log.WithContext(context.Background()))
	client.mu.Lock()
	client.reconnectCancel = cancel
	client.mu.Unlock()
	go client.ReconnectLoop(bgCtx)
	return nil
}

// GetLoginFlows returns supported login flows. We support JID+password.
func (c *JabberConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{
		ID:          "password",
		Name:        "Password",
		Description: "Log in with your Jabber ID (JID) and password",
	}, {
		ID:          "password-server",
		Name:        "Password (custom server)",
		Description: "Log in with your JID and password, connecting to a specific server (host:port or tls://host:port)",
	}}
}

// CreateLogin returns a login process for the given flow (e.g. "password").
func (c *JabberConnector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	if flowID != "password" && flowID != "password-server" {
		return nil, fmt.Errorf("unknown flow %q", flowID)
	}
	return &jabberLoginProcess{conn: c, user: user, customServer: flowID == "password-server"}, nil
}

// GetBridgeInfoVersion returns versions for bridge info and room capabilities.
func (c *JabberConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 4
}

// jabberLoginProcess implements LoginProcess and LoginProcessUserInput for JID+password auth.
type jabberLoginProcess struct {
	conn         *JabberConnector
	user         *bridgev2.User
	customServer bool // the password-server flow (asks for a server override)
}

// buildLoginMeta builds the metadata and dial options of a new login. In the custom-server flow the
// server is required ("auto" clears a stored override); the plain flow keeps any stored override.
// userLogins returns the sender's own logins (nil without a user, e.g. in tests).
func (j *jabberLoginProcess) userLogins() []*bridgev2.UserLogin {
	if j.user == nil {
		return nil
	}
	return j.user.GetUserLogins()
}

// existingDialOptions returns the stored override of the sender's own login with this ID (zero value when
// there is none), for the plain re-login flow.
func existingDialOptions(logins []*bridgev2.UserLogin, id networkid.UserLoginID) dialOptions {
	for _, ul := range logins {
		if ul != nil && ul.ID == id {
			meta, _ := ul.Metadata.(*JabberLoginMeta)
			return meta.dialOptions()
		}
	}
	return dialOptions{}
}

func buildLoginMeta(password, server string, custom bool) (*JabberLoginMeta, dialOptions, error) {
	meta := &JabberLoginMeta{Password: password}
	if !custom {
		return meta, dialOptions{}, nil
	}
	if strings.TrimSpace(server) == "" {
		return nil, dialOptions{}, fmt.Errorf("server is required (use auto for DNS discovery)")
	}
	opts, _, err := parseServerOverride(server)
	if err != nil {
		return nil, dialOptions{}, err
	}
	meta.Server, meta.Port, meta.DirectTLS = opts.Server, opts.Port, opts.DirectTLS
	meta.serverExplicit = true
	return meta, opts, nil
}

func (j *jabberLoginProcess) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	prefix := "!jabber"
	if j.conn.br != nil && j.conn.br.Config != nil && j.conn.br.Config.CommandPrefix != "" {
		prefix = j.conn.br.Config.CommandPrefix
	}
	if j.customServer {
		return &bridgev2.LoginStep{
			Type:         bridgev2.LoginStepTypeUserInput,
			StepID:       "fi.mau.jabber.password_server",
			Instructions: fmt.Sprintf("Enter your Jabber ID, password and the server to connect to. You can send them in one message: `%s login password-server <jid> <password> <server>`.\n\nAfter you submit, the bot will connect to your XMPP server; this can take 1–2 minutes—please wait for a response.", prefix),
			UserInputParams: &bridgev2.LoginUserInputParams{
				Fields: []bridgev2.LoginInputDataField{
					{Type: bridgev2.LoginInputFieldTypeUsername, ID: "jid", Name: "Jabber ID (JID)", Description: "Your full JID, e.g. user@example.com"},
					{Type: bridgev2.LoginInputFieldTypePassword, ID: "password", Name: "Password", Description: "Your Jabber account password"},
					{Type: bridgev2.LoginInputFieldTypeDomain, ID: "server", Name: "Server", Description: "host, host:port, or tls://host:port for direct TLS; auto to use DNS"},
				},
			},
		}, nil
	}
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeUserInput,
		StepID:       "fi.mau.jabber.password",
		Instructions: fmt.Sprintf("Enter your Jabber ID (e.g. user@jabber.org) and password. You can send them in one message: `%s login password <jid> <password>` or follow the prompts.\n\nAfter you submit, the bot will connect to your XMPP server; this can take 1–2 minutes—please wait for a response.", prefix),
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{
				{
					Type:        bridgev2.LoginInputFieldTypeUsername,
					ID:          "jid",
					Name:        "Jabber ID (JID)",
					Description: "Your full JID, e.g. user@jabber.org",
				},
				{
					Type:        bridgev2.LoginInputFieldTypePassword,
					ID:          "password",
					Name:        "Password",
					Description: "Your Jabber account password",
				},
			},
		},
	}, nil
}

func (j *jabberLoginProcess) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	jidStr := strings.TrimSpace(input["jid"])
	password := input["password"]

	if jidStr == "" {
		return nil, fmt.Errorf("JID is required")
	}
	if !strings.Contains(jidStr, "@") {
		return nil, fmt.Errorf("JID must be in the form user@domain (e.g. user@jabber.org)")
	}
	if password == "" {
		return nil, fmt.Errorf("password is required")
	}
	meta, dialOpts, err := buildLoginMeta(password, input["server"], j.customServer)
	if err != nil {
		return nil, err
	}
	if !j.customServer {
		// Re-login with the plain flow keeps the stored override (CopyFrom), so verify against it too.
		dialOpts = existingDialOptions(j.userLogins(), networkid.UserLoginID(strings.ToLower(jidStr)))
	}
	dialOpts.AllowPrivate = j.conn.allowPrivateServerOverride()

	ctx, span := telemetry.Start(ctx, "bridge.login.submit",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("login.jid", jidStr)))
	defer span.End()

	log := j.conn.br.Log.With().Logger()
	log = *telemetry.Logger(ctx, &log)
	warnIfDebugIdentifiers(&log)
	log.Debug().Str("jid", jidStr).Msg("XMPP login: attempting connection (this may take 1–2 minutes)")

	// Verify XMPP credentials by connecting once before creating the login.
	dialCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
	defer cancel()
	origin, err := parseJID(jidStr)
	if err != nil {
		telemetry.Fail(span, err)
		return nil, fmt.Errorf("invalid JID format (use user@domain)")
	}
	session, err := dialXMPPFunc(dialCtx, origin, password, dialOpts, &log)
	if err != nil {
		telemetry.Fail(span, err)
		log.Debug().Err(err).Str("jid", jidStr).Msg("XMPP login: connection or auth failed")
		hint := ""
		if !j.customServer {
			hint = fmt.Sprintf(" If your provider uses a different connection host, use `%s login password-server`.", j.commandPrefix())
		}
		if dialCtx.Err() != nil {
			return nil, fmt.Errorf("connection timed out (is the server reachable?).%s", hint)
		}
		// Do not wrap or echo err to avoid leaking server messages or credentials into logs.
		if _, isSASL := saslCondition(err); isSASL {
			return nil, fmt.Errorf("XMPP authentication failed")
		}
		if strings.Contains(err.Error(), errPrivateOverride) {
			return nil, fmt.Errorf("%s (allow it with network.allow_private_server_override)", errPrivateOverride)
		}
		if strings.Contains(err.Error(), errPrivateSRV) {
			return nil, fmt.Errorf("%s (allow it with network.allow_private_server_override)", errPrivateSRV)
		}
		return nil, fmt.Errorf("could not connect to the XMPP server.%s", hint)
	}
	_ = session.Close()
	log.Debug().Str("jid", jidStr).Msg("XMPP login: verification succeeded, creating bridge login")

	loginID := networkid.UserLoginID(strings.ToLower(jidStr))
	data := &database.UserLogin{
		ID:         loginID,
		RemoteName: jidStr,
		Metadata:   meta,
	}

	ul, err := j.user.NewLogin(ctx, data, nil)
	if err != nil {
		telemetry.Fail(span, err)
		return nil, fmt.Errorf("failed to create login: %w", err)
	}

	prefix := "!jabber"
	if j.conn.br != nil && j.conn.br.Config != nil && j.conn.br.Config.CommandPrefix != "" {
		prefix = j.conn.br.Config.CommandPrefix
	}
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       "fi.mau.jabber.complete",
		Instructions: fmt.Sprintf("Successfully connected to XMPP as %s. You can use `%s help` for commands (e.g. `%s pm` or `%s start-chat` with an optional login JID to open a DM from a specific account, `%s join <room-jid>` for MUC rooms).", jidStr, prefix, prefix, prefix, prefix),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: loginID,
			UserLogin:   ul,
		},
	}, nil
}

func (j *jabberLoginProcess) Cancel() {}

func (j *jabberLoginProcess) commandPrefix() string {
	if j.conn.br != nil && j.conn.br.Config != nil && j.conn.br.Config.CommandPrefix != "" {
		return j.conn.br.Config.CommandPrefix
	}
	return "!jabber"
}

// parseJID parses a JID string into a jid.JID (bare JID for dialing).
func parseJID(s string) (jid.JID, error) {
	return jid.Parse(s)
}

// SetMUCPreferredNickForRoom sets the preferred MUC nickname for a room and persists it in portal metadata.
// It is used by the !jabber muc-nick command.
func SetMUCPreferredNickForRoom(ctx context.Context, br *bridgev2.Bridge, loginID networkid.UserLoginID, roomJID, nick string) error {
	portalKey := networkid.PortalKey{ID: networkid.PortalID(roomJID), Receiver: loginID}
	portal, err := br.GetPortalByKey(ctx, portalKey)
	if err != nil {
		return err
	}
	meta, _ := portal.Metadata.(*PortalMetadata)
	if meta == nil {
		meta = &PortalMetadata{}
	}
	setExplicitMUCNick(meta, nick)
	portal.Metadata = meta
	return portal.Save(ctx)
}

// saveExplicitMUCNickOn stores nick as the explicit per-room nick of the portal key.
func saveExplicitMUCNickOn(ctx context.Context, br *bridgev2.Bridge, key networkid.PortalKey, nick string) error {
	return SetMUCPreferredNickForRoom(ctx, br, key.Receiver, string(key.ID), nick)
}

// SanitizeMUCNick is sanitizeMUCNick for the command layer.
func SanitizeMUCNick(roomBare, raw string) (nick, reason string) {
	return sanitizeMUCNick(roomBare, raw)
}

// SetDefaultMUCNick sets the default MUC nickname for the login and persists it in user login metadata.
// It is used by the !jabber muc-nick-default command.
func SetDefaultMUCNick(ctx context.Context, br *bridgev2.Bridge, login *bridgev2.UserLogin, nick string) error {
	extraWriteMu.Lock() // the save marshals the whole metadata, including Extra written by other commands
	defer extraWriteMu.Unlock()
	meta, _ := login.Metadata.(*JabberLoginMeta)
	if meta == nil {
		meta = &JabberLoginMeta{}
	}
	meta.DefaultMUCNick = nick
	login.Metadata = meta
	return login.Save(ctx)
}
