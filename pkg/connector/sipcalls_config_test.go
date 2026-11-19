//go:build sipcalls

package connector

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"

	"github.com/williamkray/mrtc-sip-bridge/pkg/calls"
	"github.com/williamkray/mrtc-sip-bridge/pkg/hostkit"
	"github.com/williamkray/mrtc-sip-bridge/pkg/rtc"
	"github.com/williamkray/mrtc-sip-bridge/pkg/sipaccount"
)

// testCallsExt returns a calls extension of a fresh connector over a minimal bridge (no Matrix, no DB).
func testCallsExt(t *testing.T, cheogram bool) (*callsExt, *JabberConnector) {
	t.Helper()
	t.Setenv("JMPCHAT_BRIDGE_CHEOGRAM", "")
	t.Setenv("MAUTRIX_JABBER_CHEOGRAM", "")
	// Commands are registered by sipcalls_commands.go; they need a real processor, which these tests
	// do not have.
	saved := registerCallsCommands
	registerCallsCommands = nil
	t.Cleanup(func() { registerCallsCommands = saved })
	c := &JabberConnector{}
	c.SetNetworkConfig(&JabberConfig{Cheogram: cheogram})
	br := &bridgev2.Bridge{Network: c, Log: zerolog.Nop(), Config: &bridgeconfig.BridgeConfig{CommandPrefix: "!jc"}}
	e, ok := c.extension().(*callsExt)
	if !ok {
		t.Fatal("calls extension not registered")
	}
	e.init(br)
	c.br = br
	return e, c
}

// networkNode parses a `network:` section body.
func networkNode(t *testing.T, body string) *yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Content) == 0 {
		return &yaml.Node{}
	}
	return doc.Content[0]
}

// yamlKeys returns the yaml keys of t's fields (inline structs flattened).
func yamlKeys(t reflect.Type) []string {
	var keys []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("yaml")
		if strings.Contains(tag, "inline") {
			keys = append(keys, yamlKeys(f.Type)...)
			continue
		}
		if name, _, _ := strings.Cut(tag, ","); name != "" && name != "-" {
			keys = append(keys, name)
		}
	}
	return keys
}

func TestCallsExampleConfig(t *testing.T) {
	c := &JabberConnector{}
	example, _, _ := c.GetConfig()
	if !strings.Contains(example, "\n"+callsExampleConfig()) {
		t.Fatalf("calls block not appended to the example:\n%s", example)
	}
	node := networkNode(t, example)
	cfg, err := decodeCallsConfig(node)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Enabled {
		t.Fatal("example must have calls disabled")
	}
	var standalone hostkit.Config
	if err = yaml.Unmarshal([]byte(hostkit.ExampleYAML), &standalone); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg.Config, standalone) {
		t.Fatalf("calls example differs from the standalone example:\n got %+v\nwant %+v", cfg.Config, standalone)
	}
	// Every key is present under calls:.
	var doc struct {
		Calls map[string]any `yaml:"calls"`
	}
	if err = yaml.Unmarshal([]byte(example), &doc); err != nil {
		t.Fatal(err)
	}
	for _, k := range yamlKeys(reflect.TypeOf(CallsConfig{})) {
		if _, ok := doc.Calls[k]; !ok {
			t.Errorf("key calls.%s missing from the example", k)
		}
	}
	// Defaults applied to the example change nothing the standalone defaults would not.
	applied := cfg
	applied.ApplyDefaults()
	if err = applied.Validate(callsConfigPrefix); err != nil {
		t.Fatalf("example does not validate: %v", err)
	}
}

func TestCallsConfigValidate(t *testing.T) {
	valid := func() CallsConfig {
		c := CallsConfig{Enabled: true}
		c.ExternalIP = "203.0.113.1"
		return c
	}
	cfg := valid()
	if err := validateCallsConfig(&cfg, true); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if cfg.SIPPortMin != 5060 || cfg.SIPPortMax != 5069 || cfg.RTPPortMin != 40000 || cfg.RTPPortMax != 40100 || cfg.RingTimeoutSeconds != 30 {
		t.Fatalf("defaults not applied: %+v", cfg.Config)
	}
	tests := []struct {
		name   string
		mutate func(*CallsConfig)
		want   string
	}{
		{"ring timeout", func(c *CallsConfig) { c.RingTimeoutSeconds = 4 }, "network.calls.ring_timeout_seconds"},
		{"sip ports reversed", func(c *CallsConfig) { c.SIPPortMin, c.SIPPortMax = 5070, 5060 }, "network.calls.sip_port"},
		{"rtp ports reversed", func(c *CallsConfig) { c.RTPPortMin, c.RTPPortMax = 40100, 40000 }, "network.calls.rtp_port"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := valid()
			tc.mutate(&c)
			err := validateCallsConfig(&c, true)
			if err == nil || !strings.HasPrefix(err.Error(), tc.want) {
				t.Fatalf("err = %v, want prefix %q", err, tc.want)
			}
		})
	}
	c := valid()
	if err := validateCallsConfig(&c, false); !errors.Is(err, errCallsNeedCheogram) {
		t.Fatalf("cheogram off: err = %v", err)
	}
}

func TestCallsConfigUpgradePreservesUserValues(t *testing.T) {
	c := &JabberConnector{}
	example, _, upgrader := c.GetConfig()
	user := "cheogram: true\ncalls:\n    enabled: true\n    external_ip: 203.0.113.9\n    sip_port_min: 5070\n" +
		"    sip_port_max: 5079\n    rtp_port_min: 40200\n    rtp_port_max: 40300\n    default_country_code: \"44\"\n" +
		"    dial_allowed_prefixes: []\n    dtmf_redact_command: false\n    outbound_trigger: membership\n" +
		"    widget:\n        enabled: true\n        public_url: https://bridge.example.com/widget/\n" +
		"        secret: 0123456789abcdef0123456789abcdef0123456789abcdef\n        frame_ancestors: [\"https://app.example.com\"]\n" +
		"        allow_remote_users: true\n        remove_when_disabled: true\n"
	out, err := configupgrade.DoBytes([]byte(user), &configupgrade.StructUpgrader{
		SimpleUpgrader: func(h configupgrade.Helper) { upgrader.DoUpgrade(h) },
		Base:           example,
	})
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cheogram bool `yaml:"cheogram"`
	}
	if err = yaml.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	cfg, err := decodeCallsConfig(networkNode(t, string(out)))
	if err != nil {
		t.Fatal(err)
	}
	var want hostkit.Config
	if err = yaml.Unmarshal([]byte(hostkit.ExampleYAML), &want); err != nil {
		t.Fatal(err)
	}
	no, yes := false, true
	want.ExternalIP, want.SIPPortMin, want.SIPPortMax, want.RTPPortMin, want.RTPPortMax = "203.0.113.9", 5070, 5079, 40200, 40300
	want.DefaultCountryCode, want.DialAllowedPrefixes, want.DTMFRedactCommand, want.OutboundTrigger = "44", []string{}, &no, "membership"
	want.Widget = hostkit.WidgetConfig{Enabled: true, PublicURL: "https://bridge.example.com/widget/",
		Secret: "0123456789abcdef0123456789abcdef0123456789abcdef", FrameAncestors: []string{"https://app.example.com"},
		AllowRemoteUsers: true, RemoveWhenDisabled: true, TrustedProxies: []string{},
		PWA: hostkit.PWAConfig{InactivityDays: 90, RotationGraceSeconds: 120, EnrollMinutes: 15, MaxLinksPerUser: 5, NotifyOnEnroll: &yes}}
	if !doc.Cheogram || !cfg.Enabled || !reflect.DeepEqual(cfg.Config, want) {
		t.Fatalf("upgrade lost values: cheogram=%v enabled=%v\n got %+v\nwant %+v\n%s", doc.Cheogram, cfg.Enabled, cfg.Config, want, out)
	}
}

func TestCallsStartConfigErrors(t *testing.T) {
	noRuntime := func(t *testing.T, x *callsExt) {
		x.newRuntime = func(context.Context, sipaccount.NetConfig, zerolog.Logger) (*sipaccount.Runtime, error) {
			t.Error("runtime must not be created")
			return nil, errors.New("unexpected")
		}
		x.newCallStack = func(*bridgev2.Bridge, *hostkit.Config, hostkit.CallStackDeps) (*calls.Manager, *rtc.ServiceURLResolver) {
			t.Error("call stack must not be created")
			return nil, nil
		}
	}
	t.Run("enabled without cheogram", func(t *testing.T) {
		x, _ := testCallsExt(t, false)
		noRuntime(t, x)
		x.postInit(nil, networkNode(t, "cheogram: false\ncalls:\n    enabled: true\n    external_ip: 203.0.113.1\n"))
		if err := x.start(context.Background()); !errors.Is(err, errCallsNeedCheogram) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("decode error", func(t *testing.T) {
		x, _ := testCallsExt(t, true)
		noRuntime(t, x)
		called := false
		registerCallsCommands = func(*callsExt, *commands.Processor) { called = true }
		x.postInit(&commands.Processor{}, networkNode(t, "calls:\n    enabled: [1, 2]\n"))
		err := x.start(context.Background())
		if err == nil || !strings.HasPrefix(err.Error(), "network.calls: ") {
			t.Fatalf("err = %v", err)
		}
		if called {
			t.Fatal("commands registered despite a config error")
		}
	})
	t.Run("invalid value", func(t *testing.T) {
		x, _ := testCallsExt(t, true)
		noRuntime(t, x)
		x.postInit(nil, networkNode(t, "calls:\n    enabled: true\n    ring_timeout_seconds: 1000\n"))
		err := x.start(context.Background())
		if err == nil || !strings.HasPrefix(err.Error(), "network.calls.ring_timeout_seconds") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("disabled", func(t *testing.T) {
		x, _ := testCallsExt(t, true)
		noRuntime(t, x)
		x.postInit(nil, networkNode(t, "cheogram: true\n"))
		if err := x.start(context.Background()); err != nil {
			t.Fatalf("err = %v", err)
		}
		if x.enabled || x.rtc != nil || x.runtime() != nil || x.manager() != nil {
			t.Fatal("disabled start created state")
		}
		if attrs := x.startAttrs(); len(attrs) != 1 || attrs[0].Value.AsBool() {
			t.Fatalf("start attrs %v", attrs)
		}
		x.stop() // must not panic without a runtime
	})
}

// A missing or "generate" widget secret becomes a random value (under network.calls.widget).
func TestCallsConfigUpgradeGeneratesWidgetSecret(t *testing.T) {
	c := &JabberConnector{}
	example, _, upgrader := c.GetConfig()
	out, err := configupgrade.DoBytes([]byte("cheogram: true\ncalls:\n    enabled: true\n"), &configupgrade.StructUpgrader{
		SimpleUpgrader: func(h configupgrade.Helper) { upgrader.DoUpgrade(h) },
		Base:           example,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := decodeCallsConfig(networkNode(t, string(out)))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Widget.Secret) < 32 || cfg.Widget.Secret == hostkit.WidgetSecretGenerate {
		t.Fatalf("secret = %q", cfg.Widget.Secret)
	}
}
