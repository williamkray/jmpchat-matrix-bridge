# Packaging for jmpchat-matrix-bridge.
#
# Same build the upstream Dockerfile performs (CGO + libopus for calls, bundled
# sqlite3, build tags `sipcalls nolibopusfile`) except that E2BE uses mautrix-go's
# pure-Go Olm implementation (`goolm` build tag) instead of libolm, which is
# unmaintained and marked insecure in nixpkgs. The goolm pickle format is
# compatible with libolm, so existing crypto state keeps working.
#
# `jmpchatSrc` defaults to this repo's checkout (flakes pass their own clean
# source); buildGoModule needs network access for the `go mod` fixed-output
# derivation.
{
  lib,
  buildGo126Module,
  pkg-config,
  opus,
  version ? "0.10.2",
  jmpchatSrc ? lib.cleanSourceWith {
    src = lib.cleanSource ./.;
    filter = name: type: type != "symlink" || baseNameOf name != "result";
  },
}:
buildGo126Module rec {
  pname = "jmpchat-matrix-bridge";
  inherit version;
  src = jmpchatSrc;

  subPackages = ["cmd/jmpchat-matrix-bridge"];
  tags = ["sipcalls" "nolibopusfile" "goolm"];
  env.CGO_ENABLED = "1";
  # libopus.a pulls in libm symbols; some toolchains need them explicit at link time.
  env.CGO_LDFLAGS = "-lm";
  buildInputs = [opus];
  nativeBuildInputs = [pkg-config];
  ldflags = ["-s" "-w" "-X main.Tag=v${version}"];
  vendorHash = "sha256-REktjICRFnbpjKKQlbbfRTLqtBeUcKYSri82GH16n1k=";

  meta = with lib; {
    description = "Matrix <-> JMP.chat bridge: XMPP/SMS/MMS, SIP calls, CardDAV contacts";
    homepage = "https://github.com/williamkray/jmpchat-matrix-bridge";
    license = licenses.mit;
    maintainers = [];
    mainProgram = "jmpchat-matrix-bridge";
    platforms = platforms.linux;
  };
}
