{
  description = "jmpchat-matrix-bridge — a Matrix <-> JMP.chat bridge (XMPP/SMS/MMS, SIP calls, CardDAV contacts)";

  inputs.nixpkgs.url = "nixpkgs/nixos-unstable";

  outputs = {
    self,
    nixpkgs,
  }: let
    # nixpkgs-unstable is needed for Go 1.26 (go.mod requires it).
    system = "x86_64-linux";
    pkgs = import (nixpkgs) {inherit system;};
    jmpchat-matrix-bridge = pkgs.callPackage ./package.nix {jmpchatSrc = self;};
  in {
    packages.${system} = {
      jmpchat-matrix-bridge = jmpchat-matrix-bridge;
      default = jmpchat-matrix-bridge;
    };

    # The overlay package builds with the pure-Go `goolm` Olm implementation
    # (mautrix-go build tag), so no nixpkgs insecure-package config is needed.
    overlays.default = final: prev: {
      jmpchat-matrix-bridge = final.callPackage ./package.nix {jmpchatSrc = self;};
    };

    # NixOS module: services.jmpchat-matrix-bridge.enable = true; (see README "Nix")
    nixosModules.default = import ./nixos-module.nix;

    checks.${system}.default = jmpchat-matrix-bridge;
  };
}
