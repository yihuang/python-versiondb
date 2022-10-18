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
    let
      overrides = { poetry2nix, rocksdb, lib }: poetry2nix.overrides.withDefaults
        (lib.composeManyExtensions [
          (self: super:
            let
              buildSystems = {
                rocksdb = [ "setuptools" "cython" "pkgconfig" ];
                pyroaring = [ "setuptools" ];
                roaring64 = [ "poetry" ];
                cprotobuf = [ "setuptools" ];
              };
            in
            lib.mapAttrs
              (attr: systems: super.${attr}.overridePythonAttrs
                (old: {
                  nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ map (a: self.${a}) systems;
                }))
              buildSystems
          )
          (self: super: {
            rocksdb = super.rocksdb.overridePythonAttrs (old: {
              buildInputs = (old.buildInputs or [ ]) ++ [ rocksdb ];
            });
          })
        ]);
      versiondb-env = { poetry2nix, lib, rocksdb }:
        poetry2nix.mkPoetryEnv {
          projectDir = ./.;
          overrides = overrides { inherit poetry2nix lib rocksdb; };
        };
    in
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
            versiondb-env = pkgs.callPackage versiondb-env { };
            versiondb-cli = pkgs.writeShellScriptBin "versiondb" ''
              ${packages.versiondb-env}/bin/versiondb $@
            '';
          };
          defaultPackage = packages.versiondb-cli;
          apps = {
            default = {
              type = "app";
              program = "${packages.versiondb-cli}/bin/versiondb";
            };
          };
          devShell = pkgs.mkShell {
            buildInputs = [ packages.versiondb-env ];
          };
        }
      )
    ) // {
      overlay = final: prev: {
        rocksdb = (prev.rocksdb.override {
          enableJemalloc = true;
        }).overrideAttrs (old: rec {
          pname = "rocksdb";
          version = "6.29.5";
          src = rocksdb-src;
        });
      };
    };
}
