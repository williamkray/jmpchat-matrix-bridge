# NixOS module for jmpchat-matrix-bridge (see flake.nix).
#
# What it does:
#   - builds the bridge with the upstream build configuration (SIP call support
#     via libopus, CGO sqlite3) with E2BE through mautrix-go's pure-Go Olm
#     implementation (`goolm` build tag — no libolm, which is unmaintained and
#     marked insecure in nixpkgs)
#   - a one-shot init unit that, on every boot:
#       1. installs config.yaml generated from `services.jmpchat-matrix-bridge.settings`
#          (first boot) or deep-merges the template into the existing state copy
#          (later boots: settings changes take effect; generated secrets like
#          as_token/hs_token are preserved — see mergePy for the merge rules)
#       2. generates the appservice registration with the bridge's own `-g`
#          (only if missing — `-g` rotates both tokens on every run, so it must
#          not be re-run while the registration is installed on the homeserver)
#   - a hardened systemd daemon unit running as a dedicated non-root user
#   - opens the configured SIP signalling and RTP media firewall ranges only
#     when network.calls.enabled is true
#   - when local Synapse is enabled, installs its registration and orders
#     matrix-synapse after the init unit; other homeservers need no local unit
{
  config,
  lib,
  pkgs,
  ...
}:
with lib; let
  cfg = config.services.jmpchat-matrix-bridge;
  stateDir = "/var/lib/jmpchat-matrix-bridge";
  # The Synapse module hardcodes its user/group names; only the data dir is
  # configurable, so follow it instead of hardcoding /var/lib/matrix-synapse.
  synapseDir = config.services.matrix-synapse.dataDir;
  registrationFile = "jabber-registration.yaml";
  synapseEnabled = config.services.matrix-synapse.enable;

  callsEnabled = attrByPath ["network" "calls" "enabled"] false cfg.settings;
  sipPorts = {
    from = attrByPath ["network" "calls" "sip_port_min"] 5060 cfg.settings;
    to = attrByPath ["network" "calls" "sip_port_max"] 5069 cfg.settings;
  };
  rtpPorts = {
    from = attrByPath ["network" "calls" "rtp_port_min"] 40000 cfg.settings;
    to = attrByPath ["network" "calls" "rtp_port_max"] 40100 cfg.settings;
  };

  yamlGenerator = pkgs.formats.yaml {};
  baseConfig = yamlGenerator.generate "jmpchat-matrix-bridge-config.yaml" cfg.settings;

  # Deep-merges the Nix template into the state config on every boot, so that
  # settings changes in the NixOS config take effect after the first install.
  # Rules:
  #   - template values win (deep for dicts, whole-value for lists/scalars)
  #   - keys only present in the state config (generated secrets like
  #     appservice.as_token/hs_token) are kept
  #   - a template value of exactly "generate" is a generation placeholder:
  #     it only fills a missing key, never overwrites a generated one
  #   - the file is only rewritten when the merge actually changed something
  mergePy = pkgs.writeText "jmpchat-matrix-bridge-merge-config.py" ''
    import sys
    import yaml

    state_path, template_path = sys.argv[1], sys.argv[2]

    def deep_merge(dst, src):
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                deep_merge(dst[k], v)
            elif v == "generate" and k in dst:
                pass
            else:
                dst[k] = v

    with open(template_path) as f:
        template = yaml.safe_load(f) or {}
    with open(state_path) as f:
        state = yaml.safe_load(f) or {}

    deep_merge(state, template)

    new = yaml.safe_dump(state, sort_keys=False, default_flow_style=False)
    with open(state_path) as f:
        old = f.read()
    if old != new:
        with open(state_path, "w") as f:
            f.write(new)
        print(f"jmpchat-matrix-bridge: merged Nix template into {state_path}")
    else:
        print("jmpchat-matrix-bridge: state config up to date")
  '';

  pythonEnv = pkgs.python3.withPackages (p: [ p.pyyaml ]);

  initScript = pkgs.writeShellScript "jmpchat-matrix-bridge-init" ''
    set -euo pipefail

    # Create the state directory with the right owner and mode explicitly.
    # The daemon's StateDirectory= also manages existing directories, but this
    # root unit must not claim the same directory itself (see below).
    install -d -m 0700 -o ${cfg.user} -g ${cfg.group} ${stateDir}
    # 1. Install the config on first boot; on later boots merge the Nix template
    #    into the state copy so settings changes take effect (secrets preserved).
    if [ ! -f ${stateDir}/config.yaml ]; then
      install -D -m 0600 -o ${cfg.user} -g ${cfg.group} ${baseConfig} ${stateDir}/config.yaml
    else
      ${pythonEnv}/bin/python ${mergePy} ${stateDir}/config.yaml ${baseConfig}
    fi

    # 2. Generate the registration + upsert its tokens into the state config.
    #    ONLY run this once: `-g` rotates both tokens every run, which would
    #    break the already-registered appservice. Delete both files to re-bootstrap.
    if [ ! -f ${stateDir}/registration.yaml ]; then
      ${cfg.package}/bin/jmpchat-matrix-bridge -c ${stateDir}/config.yaml -r ${stateDir}/registration.yaml -g
    fi
    chown ${cfg.user}:${cfg.group} ${stateDir}/config.yaml ${stateDir}/registration.yaml
    chmod 0600 ${stateDir}/config.yaml ${stateDir}/registration.yaml

    # 3. Expose the registration only when Synapse is enabled on this host.
    #    Remote and non-Synapse homeservers can read it from the state directory
    #    through their own deployment mechanism instead.
    ${optionalString synapseEnabled ''
      mkdir -p ${synapseDir}
      install -m 0640 -o matrix-synapse -g matrix-synapse \
        ${stateDir}/registration.yaml ${synapseDir}/${registrationFile}
    ''}
  '';
in {
  options.services.jmpchat-matrix-bridge = {
    enable = mkEnableOption "jmpchat-matrix-bridge (a Matrix<->JMP.chat/XMPP bridge, mautrix-style)";

    package = mkOption {
      type = types.package;
      default = pkgs.callPackage ./package.nix {};
      defaultText = "jmpchat-matrix-bridge from this flake";
      description = "The jmpchat-matrix-bridge package to run.";
    };

    settings = mkOption {
      type = yamlGenerator.type;
      default = {};
      description = ''
        The bridge config.yaml. as_token/hs_token are generated once on first boot
        (via the bridge's own `-g`), so leave them unset here. Values in settings
        are stored in the world-readable Nix store; use environmentFile together
        with env_config_prefix for secrets instead of placing them here.
      '';
    };

    user = mkOption {
      type = types.str;
      default = "jmpchat";
    };
    group = mkOption {
      type = types.str;
      default = "jmpchat";
    };

    environmentFile = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = ''
        Optional environment file for secrets (via the bridge's
        env_config_prefix) or OTEL_* telemetry settings. Must be an absolute
        path passed as a string, e.g. "/etc/jmpchat-matrix-bridge/env".
        Do not use a Nix path literal like ./env: that would copy the file
        into the world-readable Nix store, defeating its purpose.
      '';
    };
  };

  config = mkIf cfg.enable {
    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.group;
      home = stateDir;
    };
    users.groups.${cfg.group} = {};

    systemd.services.jmpchat-matrix-bridge-init = {
      description = "Bootstrap jmpchat-matrix-bridge config + registration";
      after = ["local-fs.target"];
      before = ["jmpchat-matrix-bridge.service"] ++ optional synapseEnabled "matrix-synapse.service";
      requiredBy = ["jmpchat-matrix-bridge.service"];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = initScript;
        # Deliberately no StateDirectory= here: this unit runs as root, and
        # systemd recursively chowns StateDirectory= trees to the unit's user,
        # i.e. to root — the daemon then cannot write its database or rotate
        # its logs (seen live: crash-loop at logger init). The script above
        # creates the directory with the right owner and mode instead.
        UMask = "0077";
        # The init unit has to stay root (it chowns state files and installs
        # the registration into Synapse's data dir), but needs nothing else.
        NoNewPrivileges = true;
        PrivateTmp = true;
        ProtectHome = true;
      };
    };

    systemd.services.jmpchat-matrix-bridge = {
      description = "jmpchat-matrix-bridge appservice";
      after = ["network-online.target" "jmpchat-matrix-bridge-init.service"];
      wants = ["network-online.target"];
      wantedBy = ["multi-user.target"];
      # Restart the daemon when settings change; it is ordered after the init
      # unit, which re-merges the template first, so the restart picks up the
      # new config without a reboot.
      restartTriggers = [baseConfig];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        # Relative paths in the config (e.g. zeroconfig's default file writer "./logs/")
        # resolve inside the state dir instead of the read-only cwd (/).
        WorkingDirectory = stateDir;
        StateDirectory = "jmpchat-matrix-bridge";
        StateDirectoryMode = "0700";
        UMask = "0077";
        ExecStart = escapeShellArgs [
          "${cfg.package}/bin/jmpchat-matrix-bridge"
          "-c" "${stateDir}/config.yaml"
          "-r" "${stateDir}/registration.yaml"
        ];
        Restart = "on-failure";
        RestartSec = 10;
        NoNewPrivileges = true;
        PrivateTmp = true;
        ProtectHome = true;
        ProtectSystem = "strict";
        ReadWritePaths = [stateDir];
        # AF_NETLINK: the SIP auto-detect enumerates local IPs via netlink (net.Dialer ip+net).
        RestrictAddressFamilies = ["AF_INET" "AF_INET6" "AF_UNIX" "AF_NETLINK"];
        AmbientCapabilities = "";
        CapabilityBoundingSet = "";
      };
      # Telemetry via OTEL_* stays opt-in: if environmentFile exports OTEL_*_ENDPOINT
      # the bridge picks that up; otherwise it installs nothing (verified in
      # pkg/telemetry: everything is off unless an OTLP endpoint is configured).
    };
    systemd.services.jmpchat-matrix-bridge.serviceConfig.EnvironmentFile =
      mkIf (cfg.environmentFile != null)
      (mkDefault cfg.environmentFile);

    # Synapse must not start before the registration exists (it fails on a
    # missing app service config file).
    systemd.services.matrix-synapse = mkIf synapseEnabled {
      after = ["jmpchat-matrix-bridge-init.service"];
      wants = ["jmpchat-matrix-bridge.service"];
    };

    # SIP signalling and RTP media must be reachable by JMP's servers only when
    # calls are enabled. Follow custom network.calls port ranges when provided.
    networking.firewall = mkIf callsEnabled {
      allowedTCPPortRanges = [sipPorts];
      allowedUDPPortRanges = [sipPorts rtpPorts];
    };
  };
}
