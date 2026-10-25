# Build stage (CGO required: sqlite3, libopus for calls; E2BE uses the pure-Go goolm, no libolm)
# Go >= 1.27.3 / 1.26.10: crypto/x509 blocks the compromised eMudhra intermediates (CVE-2026-111943).
FROM golang:1.27.3-alpine3.24 AS builder

RUN apk add --no-cache ca-certificates gcc g++ musl-dev sqlite-dev opus-dev pkgconf

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Build metadata (.git is excluded from the build context, so pass it in; see build.sh).
ARG GIT_COMMIT=unknown
ARG GIT_TAG=unknown
ARG GIT_REPOSITORY_URL=unknown
ARG BUILD_TIME=unknown
# goolm: mautrix-go's pure-Go Olm (reads/writes the libolm pickle format, so existing crypto state keeps working).
# sipcalls: SIP call support (off unless network.calls.enabled); nolibopusfile: hraban/opus without opusfile.
RUN CGO_ENABLED=1 go build -tags "goolm sipcalls nolibopusfile" -ldflags "-s -w \
	-X main.Commit=${GIT_COMMIT} -X main.Tag=${GIT_TAG} \
	-X main.RepositoryURL=${GIT_REPOSITORY_URL} -X main.BuildTime=${BUILD_TIME}" \
	-o jmpchat-matrix-bridge ./cmd/jmpchat-matrix-bridge

# Runtime stage (3.24; 3.19 is end-of-life and its CA bundle still trusts the distrusted emSign roots)
FROM alpine:3.24

ARG GIT_COMMIT=unknown
ARG GIT_REPOSITORY_URL=unknown
LABEL org.opencontainers.image.revision=${GIT_COMMIT} \
	org.opencontainers.image.source=${GIT_REPOSITORY_URL}

# --upgrade pulls the current CA bundle (>= 20261020: emSign ECC Root CA - G3, Root CA - C1 and ECC Root CA - C3 removed)
# and any other security fixes newer than the base image.
RUN apk upgrade --no-cache && apk add --no-cache --upgrade ca-certificates tzdata sqlite-libs opus

# Run as non-root
RUN adduser -D -h /app -s /sbin/nologin bridge && mkdir -p /data && chown bridge:bridge /data
WORKDIR /app

COPY --from=builder /build/jmpchat-matrix-bridge .
# compat: pre-rename binary path, remove in a later major
RUN ln -s jmpchat-matrix-bridge /app/mautrix-jabber
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER bridge

# Default appservice port (override in config if needed)
EXPOSE 29318
# SIP (network.calls.sip_port_min..max) and RTP (rtp_port_min..max) defaults; publish what you configure.
EXPOSE 5060-5069/udp 5060-5069/tcp 40000-40100/udp

ENTRYPOINT ["/app/entrypoint.sh"]
