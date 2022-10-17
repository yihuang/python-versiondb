{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/master";
    flake-utils.url = "github:numtide/flake-utils";
    rocksdb-src = {
      url = "github:facebook/rocksdb/v6.29.5";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-utils, rocksdb-src }:
    (flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              self.overlay
            ];
            config = { };
          };
        in
        rec {
          packages = {
            dev-env = pkgs.poetry2nix.mkPoetryEnv {
              projectDir = ./.;
            };
          };
          apps = {
            cronosd = mkApp packages.cronosd;
            cronosd-testnet = mkApp packages.cronosd-testnet;
          };
          defaultPackage = packages.cronosd;
          defaultApp = apps.cronosd;
          devShell = pkgs.mkShell {
            buildInputs = [ packages.dev-env ];
          };
        }
      )
    ) // {
      overlay = final: prev: {
        rocksdb = (prev.rocksdb.override { enableJemalloc = true; }).overrideAttrs (old: rec {
          pname = "rocksdb";
          version = "6.29.5";
          src = rocksdb-src;
        });
      };
    };
}
