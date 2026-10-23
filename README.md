# jmpchat-matrix-bridge

A Matrix <-> Jabber/XMPP puppeting bridge built for [JMP.chat](https://jmp.chat): SMS/MMS through Cheogram, SIP calls (to MatrixRTC), CardDAV contact import, and a Dialpad widget. It is also a regular Jabber/XMPP puppeting bridge (DMs, MUCs, media, reactions, replies, carbons, MAM backfill); the Cheogram, SIP and CardDAV features are optional extras you can ignore. Use your Matrix account to send and receive messages with Jabber users, with double-puppet support. PM handling is much more thorough than multi-user chat (MUC) capabilities. Not affiliated with the mautrix project; built on the mautrix-go bridgev2 library.

## Formerly mautrix-jabber / migrating

This project was called mautrix-jabber before version 0.6.0 (docs of earlier releases use the old name). Upgrading is a drop-in change:

- Your existing config, `registration.yaml`, database (including the database owner marker, which stays `megabridge/mautrix-jabber`; never change it), rooms, ghosts, the `!jabber` command prefix, the appservice id and ports keep working unchanged. No registration regeneration is needed.
- What changed: the Docker image is `williamkray/jmpchat-matrix-bridge` and the binary is `jmpchat-matrix-bridge`; paths `/app` and `/data` and the entrypoint behavior are unchanged. Update your compose `image:` and any command that invokes the binary by name.
- The old binary path `/app/mautrix-jabber` remains in the image as a compat symlink for custom entrypoints/commands; it will be removed in a later major version.
- The old image `williamkray/mautrix-jabber` gets no new tags (last: v0.5.0) and is deprecated.
- The default telemetry `service.name` is now `jmpchat-matrix-bridge`: update dashboards/monitors, or set `OTEL_SERVICE_NAME=mautrix-jabber` (or `DD_SERVICE`) to keep the old name; explicitly set values are unaffected.
- `MAUTRIX_JABBER_CHEOGRAM` is still read; `JMPCHAT_BRIDGE_CHEOGRAM` is preferred.
- Double-puppeted events now carry `fi.mau.double_puppet_source=jmpchat-matrix-bridge`, and the Matrix HTTP User-Agent changed. The `m.bridge` room state `protocol.id` becomes `github.com/williamkray/jmpchat-matrix-bridge`; existing rooms switch over gradually as their bridge info is next updated.

## Prerequisites

- Go 1.22+ (for building)
- PostgreSQL 10+ or SQLite (for the bridge database)
- A Matrix homeserver (e.g. Synapse) where you can register an appservice

## Build

Default build includes **Matrix end-to-bridge encryption (E2BE)** and requires CGO and libolm:

```bash
./build.sh
```

Or with Go (libolm dev headers must be installed):

```bash
CGO_ENABLED=1 go build -ldflags "-s -w" -o jmpchat-matrix-bridge ./cmd/jmpchat-matrix-bridge
```

To build without encryption support (no libolm): `go build -tags nocrypto -ldflags "-s -w" -o jmpchat-matrix-bridge ./cmd/jmpchat-matrix-bridge`. The Docker image is built with E2BE and SIP call support. To include [calls](#calls-sip-via-jmp) in a Go build, add the tags `sipcalls nolibopusfile` (e.g. `-tags "nocrypto sipcalls nolibopusfile"`; needs libopus dev headers and pkg-config).

## Setup

The bridge follows the same setup flow as other mautrix Go bridges: generate an example config, edit it, generate the appservice registration, register on the homeserver, then run.

### 1. Example config

Generate `config.yaml` from the built-in example:

```bash
./jmpchat-matrix-bridge -e
```

This writes `config.yaml` (or use `-c path` to choose a file). To print the example to stdout instead of writing a file, use `-c -`:

```bash
./jmpchat-matrix-bridge -e -c -
```

### 2. Edit config

In `config.yaml` set at least:

- **`database`**  
  - PostgreSQL: `type: postgres` and a real `uri` (e.g. `postgres://user:password@host/db?sslmode=disable`).  
  - SQLite (e.g. for local testing): `type: sqlite3-fk-wal` and `uri: file:jmpchat-matrix-bridge.db?_txlock=immediate`.

- **`homeserver.address`** – URL the bridge uses to reach the homeserver (e.g. `http://localhost:8008`).

- **`homeserver.domain`** – Server name (e.g. `example.com`). Must not be `example.com` if you want to generate a registration (the `-g` step refuses it as a safeguard).

- **`appservice`** – `address`, `hostname`, `port` so the homeserver can reach the bridge. The rest can stay as generated.

**Cheogram (SMS/MMS):** Set `network.cheogram: true` to resolve phone numbers and short codes to Cheogram JIDs (e.g. `!jabber pm 2121112222` → +12121112222@cheogram.com). Fallback: `JMPCHAT_BRIDGE_CHEOGRAM=true` if connector config is not available (the pre-rename `MAUTRIX_JABBER_CHEOGRAM` is still accepted).

  When Cheogram is enabled, **group MMS** is also supported. Cheogram represents group MMS conversations as a single JID whose local part is a comma-separated list of phone numbers (e.g. `+12121112222,+12023334444@cheogram.com`). The bridge treats that as one portal but creates a separate Matrix ghost for each number, so display names and roster resolution apply per participant. Outbound: `!jabber pm 212-111-2222, 2023334444, 18188675309` normalizes and sorts the numbers, then opens a DM with the combined JID. Inbound: messages from that JID are attributed to the correct sender (from a `<xmpp:JID>` body prefix when present, or the first number as fallback), and that prefix is stripped from the bridged message body.

  **Starting a DM:** Use `!jabber pm <jid>` or `!jabber start-chat <jid>` (they are the same; `pm` is an alias). With multiple Jabber accounts linked, add an optional login JID first to choose which account to use: `!jabber pm <login-jid> <contact-jid>` (e.g. `!jabber pm alice@jabber.org friend@example.com`).

### 3. Generate registration

Generate the appservice registration from your config (required for the homeserver to accept the bridge):

```bash
./jmpchat-matrix-bridge -g
```

Or with custom paths:

```bash
./jmpchat-matrix-bridge -g -c config.yaml -r registration.yaml
```

This:

- Writes `registration.yaml` (or the path from `-r`).
- Updates `config.yaml` with the new `as_token` and `hs_token`.

**`registration.yaml`** will look like:

- `id` – appservice id (from `appservice.id`).
- `url` – appservice URL (from `appservice.address`).
- `as_token`, `hs_token` – generated secrets.
- `sender_localpart` – bridge bot user.
- `namespaces.users` – regexes for `@jabberbot` and `@jabber_.*` on your `homeserver.domain`.

Ensure `url` matches the address where the bridge will listen (e.g. `http://localhost:29318` if that is your `appservice.address`).

### 4. Register on the homeserver

Add the registration file to Synapse's `homeserver.yaml`:

```yaml
app_service_config_files:
  - /path/to/registration.yaml
```

**If using end-to-bridge encryption (E2BE):** You must also add experimental features to `homeserver.yaml` and add MSC flags to the registration file (see [End-to-bridge encryption (E2BE)](#optional-end-to-bridge-encryption-e2be) below) before restarting.

Restart the homeserver. It should start without error; if it does not, check `registration.yaml` for valid YAML and correct `url`/`namespaces`.

### 5. Run the bridge

```bash
./jmpchat-matrix-bridge
```

Or with custom paths:

```bash
./jmpchat-matrix-bridge -c config.yaml -r registration.yaml
```

The bridge will listen on `appservice.hostname` and `appservice.port`. When the homeserver sends appservice transactions to `appservice.address`, the bridge should handle them without crashing.

---

## Optional: End-to-bridge encryption (E2BE)

To use **encrypted Matrix rooms** and hide messages from the homeserver. If using this with JMP.chat/Cheogram, remember that ALL SMS OVER THE CHEOGRAM BRIDGE AND PSTN IS PLAINTEXT, THIS DOES NOT MAKE YOUR CONVERSATIONS PRIVATE! This is recommended to remain disabled if you run your own matrix server for performance benefits.

To enable, configure encryption in three places: the bridge config, Synapse's `homeserver.yaml`, and the appservice registration file.

#### 1. Bridge configuration

Enable encryption in `config.yaml`:

```yaml
encryption:
  allow: true
  default: true   # enable encryption in new portal rooms
  require: false  # set true to drop unencrypted messages
  # Required: use appservice mode (MSC3202) instead of /sync
  appservice: true
  # Required: use MSC4190 for device management (required with MSC3861/MAS)
  msc4190: true
  # Recommended: enable self-signing for cross-signing support
  self_sign: true
  # Recommended: enable key sharing for better UX
  allow_key_sharing: true
  # Required: 32-byte key for encrypting keys in database (use "generate" for auto-generation)
  pickle_key: "generate"  # or a 32-byte key; do not use default in production
  # Recommended: trust on first use for better security
  verification_levels:
    receive: cross-signed-tofu
    send: cross-signed-tofu
    share: cross-signed-tofu
```

**Important:** `encryption.appservice: true` is **required** when using `encryption.msc4190: true`. Without it, Synapse will reject `/sync` requests from appservice users.

#### 2. Synapse configuration

Add the following experimental features to your Synapse `homeserver.yaml`:

```yaml
experimental_features:
  # Required: Enable MSC3202 device masquerading (for older Synapse <1.141)
  msc3202_device_masquerading: true
  # Required: Enable MSC2409 to-device messages via appservice transactions
  msc2409_to_device_messages_enabled: true
  # Required: Enable MSC3202 transaction extensions for ephemeral events
  msc3202_transaction_extensions: true
```

**Note:** For Synapse 1.141+, `msc3202_device_masquerading` may not be required, but the other two flags are still needed for appservice mode encryption.

#### 3. Appservice registration file

Add the following fields to your `registration.yaml`:

```yaml
# Required: Enable MSC2409 ephemeral events (to-device messages) via appservice transactions
de.sorunome.msc2409.push_ephemeral: true
# Required: Enable MSC4190 device management
io.element.msc4190: true
# Required: Enable MSC3202 device masquerading and transaction extensions
org.matrix.msc3202: true
```

**Important:** Use `de.sorunome.msc2409.push_ephemeral` (not `ephemeral_events`). Synapse checks for this specific key.

After updating the registration file, restart Synapse to load the new configuration.

#### 4. Expected behavior

Once configured correctly:

- Messages from Jabber will be encrypted when sent to Matrix rooms with encryption enabled
- The bridge bot's device (`@jabberbot:your.domain`) will be used to encrypt messages sent by ghost users
- You may see a client warning: **"the sender of the event does not match the owner of the device that sent it"**

This warning is **expected and harmless** with MSC3202 device masquerading. The bridge bot's device encrypts messages on behalf of ghost users, which causes clients to show this verification notice. Messages are encrypted and decrypted correctly. To suppress the warning, verify the bridge bot's device (`@jabberbot:your.domain`, device ID shown in logs) in your Matrix client.

#### 5. Troubleshooting E2BE

**Issue:** Bridge logs show "no group session created" or "Didn't get session, giving up trying to decrypt event"

- **Check:** Verify `de.sorunome.msc2409.push_ephemeral: true` is in registration (not `ephemeral_events`)
- **Check:** Verify `msc2409_to_device_messages_enabled: true` and `msc3202_transaction_extensions: true` are in Synapse `experimental_features`
- **Check:** Verify `encryption.appservice: true` in bridge config
- **Check:** Restart Synapse after registration changes

**Issue:** Synapse returns `NotImplementedError` when bridge tries to use `/sync`

- **Fix:** Set `encryption.appservice: true` in bridge config (required with MSC4190)

**Issue:** Keys are not being shared/received

- **Check:** Ensure `allow_key_sharing: true` in bridge config
- **Check:** Verify ephemeral events are enabled in registration and Synapse config (see above)
- **Check:** Bridge logs should show "Received inbound group session" when keys arrive

For more details, see [End-to-bridge encryption](https://docs.mau.fi/bridges/general/end-to-bridge-encryption.html).

## Production deployment

- **Secrets**: Prefer `env_config_prefix` in config so tokens and the database URI come from environment variables instead of the config file. Keep config files out of version control.
- **Database**: For multiple users, use PostgreSQL (`database.type: postgres`) with a restricted DB user and `max_open_conns` tuned for your deployment.
- **Process**: Run the bridge with minimal OS permissions (e.g. dedicated user, read-only config mount). Login passwords are stored in the bridge DB (JSON); protect the database and its volume with filesystem or DB ACLs.

## Docker

A read-only config (for example a mounted Secret) works: the bridge notices that it cannot rewrite the file and starts with `--no-update`. A missing config file gives a clear error and exit code 10.

Build the image:

```bash
docker build $(./build-args.sh) -t jmpchat-matrix-bridge .
```

`./build-args.sh` passes the git commit, tag, repository URL and build time into the build (the `.git` directory isn't part of the build context). They stamp the bridge version string, set the `org.opencontainers.image.revision` / `org.opencontainers.image.source` image labels, and are sent as `git.commit.sha` / `git.repository_url` on traces (used by Datadog's source code integration). A plain `docker build -t jmpchat-matrix-bridge .` still works; the version then shows `+dev.unknown` and the git metadata is omitted.

**First run (no config):** Run the container with no arguments and nothing mounted at `/data`. It prints an example config to stdout and exits:

```bash
docker run --rm jmpchat-matrix-bridge > config.yaml
```

Edit `config.yaml` (database, `homeserver.address`, `homeserver.domain`, `appservice`, `bridge.permissions`). Then generate the registration. The bridge updates the config file when generating tokens, so use a **directory mount** (not single-file mounts) to avoid "device or resource busy" on some systems:

```bash
mkdir -p ./bridge-data && cp config.yaml ./bridge-data/
docker run --rm -v "$(pwd)/bridge-data:/data:rw" \
  jmpchat-matrix-bridge -c /data/config.yaml -r /data/registration.yaml -g
```

Add `./bridge-data/registration.yaml` to your homeserver, then run the bridge with the same directory mounted at `/data`. With no arguments, the container runs the bridge using `/data/config.yaml` and `/data/registration.yaml`:

```bash
docker run -d --name jmpchat-matrix-bridge \
  -v "$(pwd)/bridge-data:/data" \
  -p 29318:29318 \
  jmpchat-matrix-bridge
```

Use `appservice.hostname: 0.0.0.0` in config so the bridge listens on all interfaces. For SQLite, set `database.uri` to `file:/data/jmpchat-matrix-bridge.db?_txlock=immediate` so the database lives in the same mounted directory. Postgres is also supported.

## Nix

This repo ships a flake: the bridge as a package, an overlay, and a NixOS module.

```bash
# build (and run) the binary
nix run .# -- --version
nix build .#jmpchat-matrix-bridge
```

Module usage (in your NixOS flake):

```nix
inputs.jmpchat.url = "github:williamkray/jmpchat-matrix-bridge";

# NixOS configuration
imports = [ jmpchat.nixosModules.default ];
services.jmpchat-matrix-bridge = {
  enable = true;
  settings = {
    homeserver = { address = "https://matrix.example.org"; domain = "example.org"; };
    appservice = { id = "jabber"; address = "http://127.0.0.1:29318";
                   hostname = "127.0.0.1"; port = 29318;
                   bot = { username = "jabberbot"; displayname = "Jabber bridge bot"; }; };
    bridge.permissions."example.org" = "user";
    database = { type = "sqlite3-fk-wal";
                 uri = "file:/var/lib/jmpchat-matrix-bridge/bridge.db?_txlock=immediate"; };
    network = { cheogram = true; calls = { enabled = true; }; };
  };
};
# When local Synapse is enabled, the module installs the registration as
# jabber-registration.yaml in Synapse's data dir (/var/lib/matrix-synapse by
# default); add that path to Synapse's app_service_config_files. For a remote
# or other homeserver, retrieve /var/lib/jmpchat-matrix-bridge/registration.yaml
# and register it yourself.
```

Notes:

- The module builds with the upstream build configuration (SIP calls via
  libopus, CGO sqlite3) but uses mautrix-go's pure-Go `goolm` implementation for
  E2BE instead of libolm, which is unmaintained and marked insecure in nixpkgs.
  No nixpkgs insecure-package config is needed; goolm's pickle format is
  compatible with libolm, so existing crypto state keeps working.
- On first boot the init unit writes `config.yaml` from `settings`; on later
  boots it deep-merges the template into the state copy, so settings changes
  take effect without a reboot (the daemon restarts when `settings` change;
  keys that only exist in the state config — generated secrets like the as/hs
  tokens — are preserved, and a template value of exactly `generate` never
  overwrites a generated value). The registration is generated with `-g` once
  and, together with the config, persists in `/var/lib/jmpchat-matrix-bridge`;
  delete both to re-bootstrap (and re-register).
- The daemon runs as a dedicated non-root user under a systemd sandbox. Only
  the short-lived init unit runs as root: it installs the config, generates
  the registration once and, for a local Synapse, copies it into Synapse's
  data dir. State (database, logs, config) lives in
  `/var/lib/jmpchat-matrix-bridge`; the directory is mode `0700`, and the
  generated config and registration are mode `0600`.
- Local Synapse integration is enabled only when `services.matrix-synapse.enable`
  is true. A local Synapse is not otherwise required.
- If `network.calls.enabled` is true, the module opens the configured SIP
  signalling (UDP+TCP, default 5060-5069) and RTP media (UDP, default
  40000-40100) ranges in the firewall. With calls disabled, it opens none.
- Values in `settings` are embedded in the world-readable Nix store. Do not put
  secrets there: use `environmentFile` with the bridge's `env_config_prefix`
  support instead. Generated appservice tokens are kept only in private state.

## Tracing (OpenTelemetry)

Tracing is optional and **off by default**. It is enabled purely by environment variables; there is no config-file setting. When no endpoint is set, nothing is installed (no exporter, goroutines or sockets), so there is no overhead. Export problems are logged as rate-limited warnings and never stop or block the bridge.

**Enabling.** Tracing turns on only when `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is non-empty. Always include the `http://` or `https://` scheme. These standard variables are read:

| Variable | Notes |
|----------|-------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | Required to enable. For `http/protobuf`, the base variable gets `/v1/traces` appended; the `TRACES` variant is used as-is. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` / `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL` | `grpc` or `http/protobuf`. **Set this explicitly.** Per the OTel spec the default is `http/protobuf`, and the bridge does not guess from the port. Any other value (e.g. `http/json`) logs a warning and disables tracing. |
| `OTEL_EXPORTER_OTLP_HEADERS`, `_TIMEOUT`, `_COMPRESSION`, `_CERTIFICATE`, `_INSECURE`, ... | Handled by the OTLP exporters themselves (also in `TRACES_` variants). |
| `OTEL_SDK_DISABLED=true` | Disable tracing without removing the rest of the config. An unparseable value logs a warning and is treated as false. |
| `OTEL_TRACES_EXPORTER` | Only `otlp` (or unset) is supported; `none` disables; other values warn and disable. Setting `otlp` alone does not enable tracing. |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | Standard sampling, e.g. `parentbased_traceidratio` / `0.25`. |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | Standard resource settings; these override the `DD_*` fallbacks below. |
| `OTEL_BSP_SCHEDULE_DELAY`, other `OTEL_BSP_*` | Batch export tuning, e.g. `1000` (ms) for faster delivery while testing. |

Resource defaults: `service.name` is `jmpchat-matrix-bridge` and `service.version` is the bridge version. `DD_SERVICE`, `DD_VERSION` and `DD_ENV` (sets both `deployment.environment.name` and `deployment.environment`) are used as fallbacks, and the `OTEL_*` variables above take precedence over them. `git.commit.sha` and `git.repository_url` come from the build metadata (see [Docker](#docker)); `DD_GIT_COMMIT_SHA` / `DD_GIT_REPOSITORY_URL` override them.

**Match the protocol to the port.** Port 4317 is gRPC and port 4318 is HTTP. If the configured protocol and port disagree (for example `http/protobuf` against `:4317`), the bridge logs a startup warning and you will see export errors in the log. On success it logs one `OpenTelemetry tracing enabled` line with the protocol, `scheme://host:port` and sampler.

### Generic OpenTelemetry collector

```bash
# gRPC (collector port 4317)
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc

# or HTTP (collector port 4318)
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

OTEL_SERVICE_NAME=jmpchat-matrix-bridge
OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=prod
```

### Datadog Agent (OTLP ingest)

Enable OTLP ingest on the Agent (APM must be enabled, which is the default; set `DD_APM_ENABLED=true` to be explicit):

```bash
DD_API_KEY=<your key>
DD_SITE=datadoghq.com
DD_APM_ENABLED=true
DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT=0.0.0.0:4317
DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT=0.0.0.0:4318
```

Then point the bridge at it (`DD_AGENT_HOST` alone does nothing):

```bash
# gRPC
OTEL_EXPORTER_OTLP_ENDPOINT=http://datadog-agent:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc

# or HTTP
OTEL_EXPORTER_OTLP_ENDPOINT=http://datadog-agent:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

DD_SERVICE=jmpchat-matrix-bridge   # optional
DD_ENV=prod                        # optional
DD_VERSION=0.1.8                   # optional
```

### docker-compose example (bridge + Datadog Agent)

```yaml
services:
  jmpchat-matrix-bridge:
    image: williamkray/jmpchat-matrix-bridge
    volumes:
      - ./bridge-data:/data
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: http://datadog-agent:4317
      OTEL_EXPORTER_OTLP_PROTOCOL: grpc
      DD_SERVICE: jmpchat-matrix-bridge
      DD_ENV: prod
    depends_on: [datadog-agent]

  datadog-agent:
    image: gcr.io/datadoghq/agent:7
    environment:
      DD_API_KEY: ${DD_API_KEY}
      DD_SITE: datadoghq.com
      DD_APM_ENABLED: "true"
      DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT: 0.0.0.0:4317
      DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_HTTP_ENDPOINT: 0.0.0.0:4318

volumes:
  bridge-data:
```

To use a plain collector instead, swap the second service for `otel/opentelemetry-collector-contrib` with an `otlp` receiver (grpc and http) and your exporter of choice, and keep the endpoint/protocol pair above consistent with the port.

### What is traced

Spans cover Matrix-to-XMPP handlers (messages, reactions, viewing a chat, identifier resolution, chat/user info, MUC join/leave), inbound XMPP stanzas (messages, carbons, MUC, Jingle notices), delivery to the bridge framework (`bridge.handle_remote_event` and message conversion share one trace with the inbound stanza), XEP-0363 uploads, OOB downloads, MAM backfill and catch-up, roster sync, avatar refresh, connect/dial, login, bot commands, and outbound Matrix HTTP calls made while handling traced work (shown as `METHOD /_matrix/...` with ids replaced by `*`).

A `bridge.start` span is emitted once at startup (handy as a smoke test). Background work (roster sync and full backfill after connect, MAM catch-up, backfill triggered by a new portal or a sent message, avatar refresh, roster pushes) starts its own trace, linked to the triggering trace where there is one. `xmpp.session` is recorded when a connection ends and covers its lifetime. When roster contacts or known portals exist, MAM catch-up produces one `mam.catchup.cycle` span about every 45 seconds; per-portal `mam.catchup.portal` spans appear only when messages were caught up or a query failed. Framework calls such as chat/user info lookups appear as small standalone traces when they happen outside a traced event.

### Privacy

Trace attributes include **full JIDs** (including Cheogram phone-number JIDs), XMPP stanza ids, Matrix room and event ids, timings, file sizes, MIME types and error messages. Message bodies, captions, reaction emoji, filenames and URLs are **never** recorded, and URLs inside error messages are reduced to scheme and host. Log output follows the same rule for message content: bodies, captions and reaction emoji are never logged, only lengths, counts and ids (JIDs, room names and nicks still appear in logs). The Matrix client library logs request bodies at debug level; the bridge replaces the body of event sends, redactions and state events (messages, retraction reasons, topics) with `<content omitted>` in every log writer, with no switch to disable it. Do not run the `trace` log level in production. Anyone with access to your trace backend can still see conversation metadata (who talks to whom, and when), so restrict access accordingly.

### Log correlation

Log lines emitted inside a sampled, traced operation carry four fields, each once:

- `trace_id` (32 lowercase hex characters) and `span_id` (16 lowercase hex characters): the OpenTelemetry convention.
- `dd.trace_id` and `dd.span_id`: decimal strings. The trace id is the low 64 bits. This is the Datadog convention.

The fields appear only on lines logged inside traced work. Other lines, and all lines when tracing is off, have no trace fields and are otherwise unchanged.

Correlation needs JSON logs on stdout:

```yaml
logging:
  writers:
    - type: stdout
      format: json
```

Datadog recognises both pairs by default (Logs > Pipelines > Preprocessing for JSON logs), so no pipeline changes should be needed. For the Logs tab service filter to match the traces, set the container label `com.datadoghq.tags.service` to the same value as `DD_SERVICE` or `OTEL_SERVICE_NAME`.

Expected in a correlated trace: XMPP message handling (text only, OOB URL, MUC, carbons, reactions), the framework's per-event lines, Matrix message handler debug lines, upload lines, and Matrix HTTP client lines made inside a handler.

Not expected: XMPP stanza receipt before mux dispatch, Serve start and stop, presence, reconnect loop, the "Roster push applied" line (the roster refresh it triggers is correlated), lines logged before a span starts, and queue-level framework errors.

**Fallback.** If the Logs side panel shows the fields but no link to the trace, go to Logs > Pipelines > Preprocessing for JSON logs and set the Trace Id attributes to only `trace_id` and the Span Id attributes to only `span_id`. Wait a few minutes, send a new message, and check again. If it still fails, try only `dd.trace_id` and `dd.span_id`.

## CardDAV contact import

The bridge can read a CardDAV address book (Nextcloud, SOGo, Radicale, Fastmail and similar) and add the phone numbers in it to your Jabber roster as Cheogram contacts (`+E164@cheogram.com`), named after the contact. It only reads the address book (never writes to it), only adds roster entries (never renames or removes), and only runs when you ask it to. Requires `network.cheogram: true`.

**Commands** (prefix `!jabber`):

| Command | Description |
|---------|-------------|
| `carddav-login` | Set up the CardDAV account. Run it in the management room without arguments; the bridge asks for the server URL, username, password and an optional address-book name (one message each; the password message is deleted). It connects and discovers the address books before saving anything |
| `carddav-sync` | Preview only: shows how many numbers would be added (and up to 20 of them), nothing is written |
| `carddav-sync apply` | Add the missing numbers to the roster (rate limited, at most 500 per run; run it again to continue). The result arrives as a later message |
| `carddav-status` | Show the server, the masked username, the address books and the last sync of this bridge run |
| `carddav-logout` | Remove the stored account. Contacts already added to the roster stay |

**Server URL**: a bare host (`dav.example.org`), a base URL, or the address book's own URL. The bridge probes the URL as an address book, then looks for the user's principal and address-book home (also through `/.well-known/carddav`). Examples:

- Radicale (explicit address book, the most reliable): `https://dav.example.org/alice/contacts/`
- Nextcloud: `https://cloud.example.org/remote.php/dav` (or just the host; use an app password when 2FA is on)
- SOGo: `https://mail.example.org/SOGo/dav/` or `https://mail.example.org/SOGo/dav/alice/Contacts/personal/`
- Fastmail: `https://carddav.fastmail.com` (app password)

iCloud's contacts live on `pNN-contacts.icloud.com` hosts; absolute links on the same registrable domain are followed, but this is untested. Google (OAuth only) is not supported.

Without an address-book name the bridge syncs every address book it finds except generated ones (Nextcloud "Recently contacted", SOGo "collected addresses"). An address book that cannot be read (for example a huge shared or LDAP-backed list that refuses a full listing) is skipped and counted; the other books are still synced. Give an address-book name at login to sync only that one.

**What is imported**: TEL numbers of individual contacts. A contact with one number is saved under its name (FN, else N, NICKNAME or ORG). With several numbers each entry is `Name (label)`: `mobile`, `work`, `home`, `main`, `other`, or the contact's own custom label (for example `John Doe (Office)`; Apple `X-ABLabel` groups are understood). A number that already is in the roster (under any name) is left alone, and a number found in several contacts is added once. Skipped, and counted in the summary: contacts without a number or name (`no_number`, `no_name`), unparsable numbers (`invalid`), short codes (`short_code`), numbers with extensions or pauses (`extension`), fax and pager numbers (`fax`), repeated numbers (`duplicate`) and cards the server sent that cannot be parsed (`bad_card`). Numbers without a leading `+` use `network.carddav.default_country_code`. vCard 2.1 is only partly supported (a bare `TEL;CELL:` parameter is salvaged). A contact deleted from the address book, or a roster entry you removed yourself, is not tracked: removed entries come back with the next `carddav-sync apply`.

Imported numbers do not get Matrix rooms until the first message arrives, and the periodic MAM catch-up skips them until then (see Troubleshooting). Each added contact makes the XMPP server push the roster item back, so a large import takes about `adds / 5` seconds and produces a roster refresh per entry. For roster-only calls, imported numbers count as known numbers.

**Config** (`network.carddav`, set by the bridge admin; all optional):

```yaml
network:
    cheogram: true
    carddav:
        allow_insecure_http: false   # allow http:// URLs (local testing only; the password is sent in clear)
        allow_private_hosts: false   # allow servers on loopback / private (RFC1918, ULA) / link-local / CGNAT addresses
        default_country_code: "1"    # for numbers without a leading +
```

By default the bridge refuses CardDAV servers on private or local addresses (and never uses proxy environment variables for CardDAV). A home-lab Radicale or Nextcloud on a private address, a Tailscale/CGNAT address or a Docker network needs `allow_private_hosts: true`; the refusal message names this setting. The limits (5000 contacts per sync, 500 adds per run, 5 adds per second, 32 MiB per response) are fixed.

**Credentials** are stored in plaintext in the bridge database (login metadata `extra.carddav`), the same trust boundary as the Jabber password and SIP credentials; use an app-specific password where the provider offers one. The credentials are only sent to the host you entered (or one on the same registrable domain). Commands that take the password are only accepted in the management room; the password message is redacted. Spans and logs contain counts, outcomes and the server host, never names, usernames, passwords, URL paths or vCard content.

## Calls (SIP via JMP)

Calls to and from JMP/Cheogram numbers ring in the same Matrix room as the texts, using MatrixRTC (Element Call). The call core is [mrtc-sip-bridge](https://github.com/williamkray/mrtc-sip-bridge), embedded in this bridge (build tag `sipcalls`, included in the default Docker image). Calls are off unless `network.calls.enabled: true`; the full user and host documentation is in [docs/EMBEDDING.md](https://github.com/williamkray/mrtc-sip-bridge/blob/main/docs/EMBEDDING.md).

**Requirements**

- MatrixRTC with LiveKit (Element Call) set up on the homeserver.
- Double puppeting for the calling user (see above).
- Unencrypted portals (the call rooms must not use Matrix encryption).
- `network.cheogram: true` and JMP SIP credentials for the account.

**Enable**: set `network.calls.enabled: true` and review the rest of the `network.calls` section in the example config (`./jmpchat-matrix-bridge -e`).

**Commands** (prefix `!jabber`):

| Command | Description |
|---------|-------------|
| `sip-login` | Store JMP SIP credentials (run in the management room) |
| `sip-logout` | Remove the stored SIP credentials |
| `sip-status` | Show SIP registration status (including the roster-only setting) |
| `roster-only-calls on\|off\|status` | Ring for incoming phone calls only from numbers saved in your Jabber roster; other SIP callers are rejected (480, so JMP sends them to voicemail) and a one-line notice naming the number is posted in your management room. Off by default. Gates SIP calls only: XMPP (XEP-0353) call notifications still appear. If the roster has not loaded yet, calls ring normally |
| `call` | Start a call in the current portal |
| `hangup` | End the current call |
| `dtmf` | Send DTMF digits during a call |
| `widget` | Add the Dialpad widget to the room |
| `dialpad-link`, `dialpad-links`, `dialpad-revoke` | Create, list and revoke standalone Dialpad links |

Roster-only calls uses the bridge's Jabber roster as the list of known numbers (Cheogram `+E164@cheogram.com` entries, named or not). Save a number from inside its chat room with `save-contact <name>` (or `add-to-roster <name>`), or from anywhere with `add-to-roster <jid> <name>`. Remove a contact with `remove-from-roster` inside its chat room, or `remove-from-roster <jid-or-phone>` from anywhere. Enabling it with no phone contacts in the roster works but warns that every call would go to voicemail.

**Ports**: publish the SIP port range (`network.calls.sip_port_min`..`sip_port_max`, default 5060-5069, udp and tcp) and the RTP range (`rtp_port_min`..`rtp_port_max`, default 40000-40100/udp). The Docker image exposes these defaults; publish whatever you configure.

**Dialpad widget/app**: the widget (and the standalone links from `dialpad-link`) lets you send DTMF tones from a keypad during a call. In this bridge it can also message a number (opens the number's chat room, with an "Open chat" link), save a number to your Jabber roster under a name (the same as `save-contact`; a name typed with Call is saved before the call), and toggle roster-only calls ("Only ring for contacts"). A **Contacts** tab (next to Dial, in the widget and the standalone app) lists your saved phone contacts, searchable by name or digits, with **Call** and **Message** on each row; only named Cheogram numbers appear (numbers saved without a name and ordinary Jabber contacts do not), and the list is kept in memory only. A saved name replaces an existing roster name. These controls work while the login has a registered SIP account, and Message and Save accept numbers the call allow-list refuses (short codes: use `pm`). Serve it from a dedicated hostname rather than sharing one with other web apps. See the mrtc-sip-bridge docs for setup.

**Known limits**: Element X cannot pick up outbound calls started from Matrix due to an [Element Call notification bug](https://github.com/element-hq/element-call/blob/8be229a438dc363972bdae5f6198607998a851d8/src/state/CallViewModel/CallNotificationLifecycle.ts#L128); use Element Web/Desktop for those.

## Multi-user chats (MUC)

`!jabber join [<login-jid>] <room@service>` joins a MUC and creates its Matrix room (the optional login JID picks the account when you have several; see below). The room is always created as a group chat. Without the command (for example a room first seen through an inbound message), the bridge asks the server with disco#info, falling back to a name heuristic (`@conference.`, `@muc.`, `@groups.` for Snikket) when the server cannot be asked.

A room joined before this was fixed (for example on a Snikket `groups.` domain) may have been created as a one-to-one chat; its outgoing messages then fail. The bridge does not migrate these. Run bridgev2's `!jabber delete-portal` inside that Matrix room, then `!jabber join <room-jid>` again.

**Nick conflicts.** If the preferred nick (`muc-nick`, else the nick of the room's bookmark, else `muc-nick-default`, else the login localpart) is taken in a room (409), the bridge tries `<nick>_1`, `_2` and `_3` in turn, each only after the previous one was refused. Once a join with an alternative nick is confirmed, the bridge bot posts a notice in the room's Matrix room (if it exists) and in your management room, naming the nick it ended up with and how to pick another (`!jabber muc-nick <room> <nick>`). If all nicks are taken it posts a failure notice instead. A working alternative is remembered per room (separately from a nick you set yourself) and used on reconnects and restarts, for as long as the preferred nick is unchanged; setting `muc-nick` or changing `muc-nick-default` makes the bridge try the preferred nick again.

**Auto-rejoin.** After every connect (startup or reconnect) the bridge re-joins, in the background and one room every 0.5 s (at most 200 per connect), each MUC of that account that has a Matrix room, so the room is live (and your presence, messages and avatars flow) without sending into it first. Cheogram numbers and group MMS are never joined. The normal join path is used, so nick choice and conflict handling apply and a remembered working nick does not produce new notices. The join asks the room for the history since the newest message the bridge has for it (at most 7 days and 100 messages back; the last 20 messages when there is none), so messages sent while the bridge was offline arrive once; already bridged messages are dropped as duplicates. `!jabber leave <room>` marks the room as left: it is not re-joined automatically, and neither sending into nor viewing its Matrix room brings it back (sending fails with a notice); only `!jabber join <room>` does. One info line per connect reports how many rooms were joined and skipped.

**Bookmarks (XEP-0402).** The bridge keeps your XMPP bookmarks (PEP node `urn:xmpp:bookmarks:1`, shared with Gajim, Conversations, Cheogram, Dino and others) in step with the rooms it bridges. It needs a server with PEP (Prosody, Snikket, ejabberd, ...); the check runs once per connection and one info line, `Bookmarks capability gate`, shows what the server supports (`read_supported`, `write_supported`, `compat_seen`). Without PEP nothing below happens. Without `pubsub#publish-options` the bridge reads bookmarks but never writes them (it would not be able to make the node private).

- *Writing.* `!jabber join <room>` creates the room's bookmark (or turns auto-join back on) once the join is confirmed, and `!jabber leave <room>` sets auto-join off but keeps the bookmark (nothing is ever deleted, and `leave` never creates one). Writes preserve everything other clients stored (name, password, extensions) and never overwrite an existing nick. A nick the bridge only got as a 409 alternative (`alice_1`) is not written, so your other clients keep their own. Sending, viewing and auto-rejoin never write.
- *Reading at connect.* The bookmark fetch comes first in the connect routine. Rooms whose bookmark has auto-join off are not re-joined (they are marked left and you get a notice); rooms with auto-join on that have no Matrix room yet are joined (last 20 messages) and get one, at most 50 per connection. If the bookmarks cannot be fetched the bridge behaves as it did before bookmarks existed.
- *Live changes.* The bridge advertises entity capabilities (XEP-0115, including `urn:xmpp:bookmarks:1+notify`) so the server pushes bookmark changes. Turning auto-join on in another client joins the room (and creates its Matrix room if needed); turning it off, or deleting the bookmark, makes the bridge leave the room, mark it left, keep its Matrix room and post a notice ("Left <room>: its bookmark was set to not auto-join (or was deleted) on another client. Use !jabber join <room> to return."). Note that Conversations and Cheogram set auto-join off when a group chat is left or archived on the phone. Live imports share the 50-room budget per connection; when it is used up you get one notice, and `!jabber bookmarks-sync` (or a reconnect) continues.
- *Nick and password.* A room's join nick is, in order: your `muc-nick` for that room, the bookmark nick, the remembered 409 alternative (as long as its base is unchanged), `muc-nick-default`, the login localpart. A bookmark nick changed in another client is used the next time the bridge joins that room (on reconnect, or with `!jabber join`); sending into or viewing a room the bridge is already in keeps the nick it currently holds there, so a shared nick renamed in another client is not renamed back. The bookmark's room password is sent in the join presence. When you and the bridge share a nick, messages you write from another client under it reach Matrix as sent by you; if the nick is renamed (by you or the bridge) the bridge follows the rename.
- *One-time backfill.* Once per account, after the first connect with this version (and only when the server allows private writes), the bridge creates a bookmark with auto-join on, using the nick it currently uses, for every room it already bridges that has none, and posts one notice in the management room. This makes your other clients start auto-joining those rooms. Bookmarks that already exist are never modified by it. `!jabber bookmarks-sync push` runs it again on demand.
- *Deleting a portal.* `!jabber delete-portal` (and `delete-all-portals`) only resets the bridge's Matrix side; it never leaves the XMPP room or changes a bookmark. A deleted MUC portal whose bookmark still has auto-join on is joined and recreated fresh at the next connect (or when any client re-saves the bookmark). To leave a room on all your clients, use `!jabber leave <room>` (or leave it in your other clients).
- *`!jabber bookmarks-sync [<login>] [push]`* imports the auto-join bookmarks that have no Matrix room yet (same budget), never leaves a room and never changes a bookmark; the reply gives counts only. Look for the log lines `Bookmarks: published`, `Bookmarks: imported room`, `Bookmarks: PEP event received`, `Bookmarks: backfill finished` and `Bookmarks: import finished`.

**Choosing the account.** With several Jabber logins, these commands accept the login JID as an optional first argument: `sync-roster`, `add-to-roster`, `save-contact`, `remove-from-roster`, `search-roster`, `join`, `leave`, `bookmarks-sync`, `muc-nick` and `muc-nick-default` (for example `!jabber join alice@jabber.org room@conference.example.com`). Only your own logins match, case-insensitively. Without it the command uses the account of the chat room you run it in, or else your default login (the first by ID); in the management room the reply then says which account was used. The argument is not taken as an account if it would leave the command without a needed argument, so `remove-from-roster me2@x.org` still means the contact `me2@x.org` (also inside a chat), and `add-to-roster` only takes an account in front of a contact JID, so `add-to-roster me2@x.org Some Name` still means the contact `me2@x.org`; to add your own other account, name the login first: `add-to-roster me1@x.org me2@x.org Name`. `!jabber list-logins` shows your logins. Other commands (`carddav-*`, `sip-*`, `roster-only-calls`) always use the room's or the default login.

**Avatars.** Contact avatars (vCard, XEP-0054, signalled by the XEP-0153 presence hash) are bridged one way, XMPP to Matrix; the bridge never publishes your avatar. Room occupants get an avatar too, with these rules: the occupant's room presence must carry an XEP-0153 hash, and the server must either route a vcard-temp request sent to the occupant (Prosody and Snikket do, answered from the occupant's server) or show the occupant's real JID (non-anonymous room) so the bridge can ask that. An avatar appears after the occupant has spoken at least once while the bridge is connected (no ghosts are created for silent occupants, so joining a large room costs nothing beyond a small in-memory table), and it updates or is removed when their presence advertises a different hash. In a non-anonymous room, when the same person changes their account avatar and your 1:1 contact ghost updates, their room ghosts update too, without another fetch (the room often keeps advertising the old hash until the client resends presence). Since XMPP has no per-room avatars, in a non-anonymous room your 1:1 contact's avatar wins over the hash the room advertises: an occupant seen later (for example after a restart and rejoin) gets the contact's avatar even if the room still advertises an old one. When the room advertises a different avatar than the contact has, the bridge re-checks the contact's vCard (at most once per contact every 10 minutes, and once per advertised avatar every 6 hours), and a change found there updates the room ghosts. Photos over 1 MiB or that are not an image (png, jpeg, gif, webp) are ignored, for contacts and occupants alike. Fetches are serial and paced (at most 4 per second per account); a room whose server keeps refusing is skipped until the next reconnect. There is no setting for this.

`!jabber avatar-probe [<login-jid>] <room@service/nick | jid>` is a diagnostic for missing avatars: it sends only IQ get queries (vcard-temp, XEP-0084 metadata and data, disco#info) to that one JID, never a message or presence, and replies with the timing and outcome of each (no image content is logged or shown).

## Verifying

1. **Registration** – `registration.yaml` is valid YAML and has `id`, `url`, `as_token`, `hs_token`, `sender_localpart`, and `namespaces.users` with `exclusive: true` and regexes that match your `username_template` (e.g. `@jabber_.*`). If using E2BE, verify `de.sorunome.msc2409.push_ephemeral: true`, `io.element.msc4190: true`, and `org.matrix.msc3202: true` are present.

2. **Synapse** – After adding the registration and restarting, Synapse starts and does not reject the file. If using E2BE, verify experimental features (`msc2409_to_device_messages_enabled`, `msc3202_transaction_extensions`) are enabled in `homeserver.yaml`.

3. **Bridge** – `./jmpchat-matrix-bridge` starts, binds to the appservice port, and does not panic when it receives `/transactions` from the homeserver. If using E2BE, check logs for "End-to-bridge encryption is in appservice mode" and verify no `NotImplementedError` from Synapse.

**Local run (no Synapse):** use a config with `database.type: sqlite3-fk-wal` and `database.uri: file:mautrix-jabber-verify.db?_txlock=immediate`, `homeserver.address` and `homeserver.domain` set, and `bridge.permissions` including your domain. Generate registration with `-g -c config.verify.yaml -r registration.verify.yaml`, then run `./jmpchat-matrix-bridge -c config.verify.yaml -r registration.verify.yaml`. The bridge will start, listen on the appservice port, and retry the homeserver (connection refused if Synapse is not running). Full `/transactions` handling can only be checked with a real homeserver.

## Troubleshooting

### Inbound messages not appearing in Matrix

- The bridge handles both **normal** and **chat** message types; many servers (e.g. Snikket) use `chat` for 1:1. If you still see no messages, check logs for `"XMPP message received (text only), queued for bridge"` and `"XMPP message receiver (Serve) started"`.

### Same JID logged in from bridge and another client

When the same Jabber account is used by the bridge and another app (e.g. Gajim, Conversations, Movim), messages from that other client should appear in Matrix via **Message Carbons (XEP-0280)** (real-time) and **MAM catch-up** (~45s polling). Backfill on room open is also supported. If messages are missing: check logs for "Carbons enable failed" (some servers reject carbons); ensure your server supports MAM and archives messages.

**Cheogram numbers and MAM:** to keep large rosters cheap (for example hundreds of imported phone numbers), the periodic MAM catch-up and the post-connect full backfill skip roster entries that are Cheogram 1:1 number JIDs (`+E164@cheogram.com`) unless the number already has a Matrix room or was seen on this connection (after a reconnect only the room check applies; new portals join catch-up immediately). Normal XMPP contacts, group MMS and short codes are unaffected. Known gap: a roster-named number without a Matrix room that texted while the bridge was disconnected, delivered to another client (so not to offline storage), is no longer recovered on reconnect; it appears when the next message arrives. Numbers saved with `save-contact` before any chat are in this class.

## Status

- **DM bridging** – JID+password login, 1:1 chats, portal creation on first message, identifier resolution (`!jabber pm` / `!jabber start-chat` with optional login JID for multi-account).
- **E2BE** – Build includes libolm; enable via `encryption.allow` and related config.
- **Implemented** – Reconnection with backoff (a login the server rejects, for example after a password change, switches the bridge state to bad credentials, posts a notice in the management room and stops reconnecting until you log in again), XEP-0199 keepalive (a silent connection is pinged after 90 s and reconnected when the server does not answer; server pings are answered; the bridge also pings its own address every 3 minutes so a session the server stops delivering to is reconnected), IPv6/dual-stack, DNS SRV lookup (`_xmpps-client`, `_xmpp-client`) with XEP-0368 direct TLS and a per-login server override (`!jabber login password-server <jid> <password> <server>`, where server is `host`, `host:port`, `tls://host:port` or `auto`; the certificate is always verified against the JID domain; overrides, and SRV targets on ports other than 5222/5223, that resolve to private/loopback addresses need `network.allow_private_server_override: true`), media (Matrix↔XMPP), reactions (single per user), quotes/replies (in MUCs the reply/reaction id is the room's stanza-id, resolved room-scoped; the XEP-0428 quote fallback is removed when the original is known; DM/MUC live messages only, carbons and MAM do not carry replies yet), contact roster and display names (`!jabber add-to-roster`, `!jabber save-contact`, `!jabber remove-from-roster`, `!jabber sync-roster`), MUC (join/leave, message routing, auto-rejoin, XEP-0402 bookmark sync), MAM (XEP-0313) backfill and periodic catch-up (~45s), Message Carbons (XEP-0280), typing (XEP-0085), read markers (XEP-0333) and delivery receipts (XEP-0184) in both directions (contacts' typing, read and delivery state show in Matrix; yours are sent to contacts that support them, never to Cheogram SMS/MMS numbers; each can be switched off with `network.send_typing`, `send_read_markers`, `send_delivery_receipts` and `muc_read_markers`, all default true; `muc_read_markers` tells every occupant of a group chat which message you read, and group chat markers are only sent for rooms the bridge is currently in). When backfill is enabled, **full backfill** runs for roster DMs after each connect and on `!jabber sync-roster` (so portals get history before new messages; sync-roster runs it without re-login). See [ROADMAP.md](ROADMAP.md) for phase summary.
- **Retractions (XMPP → Matrix)** – XEP-0424 message retraction and XEP-0425 moderation redact the bridged Matrix message (the standard "message deleted" placeholder is shown; a moderation or retraction reason becomes the redaction reason). The legacy `urn:xmpp:message-retract:0` forms are parsed best-effort (inbound only). A retraction is only honoured from the original sender (1:1 bare JID, MUC occupant-id or occupant JID) or, for moderation, from the room itself; the retracted ids are remembered (`jabber_retracted:*` rows in the bridge key-value table, never expired) so MUC history replay, MAM catch-up and backfill do not bring the message back. Known gaps: moderation that happens while the bridge is offline (MUC history is not archived/queried), a retraction that arrives before its original, and the `->>` JSON lookup is only tested against SQLite locally (Postgres jsonb uses the same operator). 
- **Corrections (XEP-0308, both ways)** – an inbound correction (live, message carbons, MAM catch-up/backfill) edits the bridged Matrix message instead of arriving as a new one; it is only honoured from the original sender (same ownership rules as retractions), is ignored when the original is unknown, retracted or a file/caption, and applied corrections are remembered (`jabber_corrected:*` key-value rows, never expired) so history replays and overlapping MAM windows do not re-apply them; a correction of a previous correction is resolved back to the original. Editing one of your own text messages in Matrix sends a `<replace>` correction (always naming the original id; DMs and MUCs). Editing a message to a Cheogram/SMS number sends the new text as a new message prefixed with `* ` (SMS cannot be corrected; the Matrix edit stands). Known gaps: corrections of files/captions are ignored in both directions, rows stored before the `media` metadata flag existed are treated as text, a correction that arrives before its original is ignored, a MUC correction of your own message needs double puppeting to appear as an edit, and in FetchMessages batch backfill a correction whose original is outside the batch is dropped.
- **MUC subject and encrypted messages** – a live subject change of a room (XEP-0045, a groupchat message with `<subject>` and no body) updates the Matrix room topic, posted by the bridge bot (an emptied subject clears the topic; the room re-sends the subject on every join and an unchanged topic is ignored). The topic rule is the same on resync: the subject when set, else the room description (so a resync after clearing the subject falls back to the description). OMEMO, OpenPGP (XEP-0027/0373), other messages marked with XEP-0380 and `aesgcm://` encrypted files (XEP-0454) are not decrypted: the bridge posts a fixed notice (`m.notice`, "This message was end-to-end encrypted ... Open it in an XMPP client.") instead of the sender's fallback text, never downloads the file, and keeps the message ids so replies, retractions and corrections still map. Body-less key-transport messages are dropped. Matrix messages that only carry an HTML `formatted_body` are sent as plain text.
- **Redactions (Matrix → XMPP)** – redacting one of your own messages sends an XEP-0424 retraction (`urn:xmpp:message-retract:1`, with the XEP-0428 fallback body, a store hint and an origin-id). In 1:1 chats it names the message's origin-id/id; in a MUC it names the room-assigned stanza-id, which the bridge learns from the room's echo of your message (a redaction within a few seconds of sending waits up to 3 s for it, then fails with a notice; messages sent before this feature existed, or in rooms without XEP-0359 stanza-ids, cannot be retracted). Only your own messages can be retracted (redacting someone else's message keeps the Matrix redaction and posts a notice; Matrix-side moderation via XEP-0425 is not implemented). SMS/MMS (Cheogram) messages cannot be unsent: nothing is sent, the Matrix redaction stands and the bridge bot posts a notice. Uploaded files are not deleted from the XMPP upload service.
- **Planned** – Unimplemented work is listed in [To-do](#to-do-unimplemented-work) below.

### Double puppeting

Automatic double puppeting lets the bridge use the user's real Matrix account (so messages from other clients are bridged as the user, not the ghost). It requires a **separate** appservice registration on the homeserver (e.g. `doublepuppet-registration.yaml`) with `url` null and a generated `as_token`. The bridge does not load that file; you must set the **same** token in the bridge config under `double_puppet.secrets` for your homeserver domain, in the form `"as_token:<token>"`. See [Double puppeting - mautrix-bridges](https://docs.mau.fi/bridges/general/double-puppeting.html).

## To-do (unimplemented work)

Canonical list of pending work; completed items have been removed. Order is approximate (next focus first, then phase order). For phased plan and implementation notes, see [ROADMAP.md](ROADMAP.md).

1. **Reaction target for XMPP-originated messages** — Reactions in Jabber to another user’s XMPP-originated message (e.g. in MUC) sometimes do not bridge to Matrix. Message metadata (`XMPPMessageID`, stanza-id, origin-id, occupant-id) is now persisted synchronously through `ConvertedMessagePart.DBMetadata`; reaction lookup still uses a 30-day scan, which could be replaced by `findMessagesByXMPPRef`.
2. **Multiple reactions per user per message** — Today only one reaction per user per message is shown on the other side (Matrix↔Jabber). To preserve multiple: Matrix→Jabber must send full reaction set in one XEP-0444 update (query existing from DB); Jabber→Matrix may need deduplication/event-id handling so each emoji is accepted.
3. *(done: live MUC subject updates the room topic, and resync uses the same subject-else-description rule.)*
4. **At-mentions (2.4)** — Map Matrix mentions (`@user:server`) to XMPP (e.g. JID or MUC nick) and XMPP mentions to Matrix. Deprioritized; deferred.
5. **OMEMO (4.1)** — XEP-0384; decrypt/encrypt on XMPP side in message and inbound paths; key storage. DM first; MUC later if desired. (Encrypted messages already arrive as a notice; decryption is still to do.)
6. **Roster isolation (2.1b, multi-user hardening)** — Ensure roster commands (`add-to-roster`, `save-contact`, `sync-roster`) and roster-derived data (ghost display names, DM room names) are strictly isolated per bridged Jabber ID and per Matrix user. When multiple accounts are bridged, roster data from one login must not affect another. Fix mixup behavior where roster or display names from one account leak or override another.
7. *(done: SRV lookup, direct TLS and the per-login override are implemented; see `login password-server`.)*
8. **Archive-wide MAM catch-up** — Replace the per-contact MAM polling with one archive-wide query per cycle (no `with`, RSM `after` the last seen id), routed by peer. That is O(1) IQs regardless of roster size and would close the Cheogram-number gap described under Troubleshooting.
9. **CardDAV follow-ups** — DNS SRV discovery for CardDAV, tombstones for roster entries removed on purpose, `ResolveCheogramIdentifier` switched to `phone.Normalize` (fixes `15551234567` becoming `+115551234567` and rejected `(555) 123-4567`, and honours the configured country code for non-NANP setups), an optional bulk roster mode that suppresses the per-push refresh, and call history in the Dialpad widget (the Contacts tab is done).
10. **XEP-0198 stream management (deferred)** — Resume and re-send of unacknowledged messages after a short disconnect. Today a message sent just before the connection drops can be lost without notice; keepalive and reconnect are in place, but delivery is not acknowledged at the stream level.
11. **XEP-0049 bookmark import (optional)** — A read-only import of legacy private-storage bookmarks for servers without PEP/XEP-0402 bookmarks (no writes). XEP-0048 is not implemented on purpose: servers that matter sync it with XEP-0402 themselves.

## Contributing

Code optimization recommendations (duplication, library overlap, configuration, structure, clarity) are tracked in [CODE_OPTIMIZATION.md](CODE_OPTIMIZATION.md).

## Links

- [Bridge setup (Go)](https://docs.mau.fi/bridges/go/setup.html)
- [Registering appservices](https://docs.mau.fi/bridges/general/registering-appservices.html)

### Alternative projects (inspiration and reference)

- **[Koishi](https://github.com/jjj333-p/koishi)** — XMPP ↔ Matrix bridge in Python; useful for feature and protocol ideas (e.g. puppeting, media, reply bridging, read receipts) even though the stack differs from this bridge (Go, mautrix bridgev2, mellium).

## License

MIT, see [LICENSE](LICENSE). The main dependencies are used unmodified under their own licenses: [mautrix-go](https://github.com/mautrix/go) and [go.mau.fi/util](https://github.com/mautrix/go-util) (MPL-2.0), [mellium](https://mellium.im/) `xmpp`, `sasl` and `xmlstream` (BSD-2-Clause), and [mrtc-sip-bridge](https://github.com/williamkray/mrtc-sip-bridge) (MIT).
