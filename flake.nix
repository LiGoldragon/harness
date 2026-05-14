{
  description = "Interactive harness abstraction for Persona.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";

    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";

    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system);
      mkContext =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          toolchain = fenix.packages.${system}.stable.withComponents [
            "cargo"
            "rustc"
            "rustfmt"
            "clippy"
            "rust-src"
          ];
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          src = craneLib.cleanCargoSource ./.;
          commonArgs = {
            inherit src;
            strictDeps = true;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          cargoTest =
            testTarget: testName:
            craneLib.cargoTest (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoTestExtraArgs = "--test ${testTarget} ${testName} -- --exact";
              }
            );
        in
        {
          inherit
            pkgs
            toolchain
            craneLib
            commonArgs
            cargoArtifacts
            cargoTest
            ;
        };
    in
    {
      packages = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              pname = "persona-harness";
              meta.mainProgram = "persona-harness-daemon";
            }
          );
        }
      );

      checks = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
            }
          );
          harness-identity-projection-views =
            context.cargoTest "smoke" "harness_identity_projection_keeps_full_owner_view";
          harness-identity-projection-source-constraint = context.cargoTest "actor_runtime_truth"
            "harness_identity_projection_cannot_leak_everything_by_default";
          harness-kind-closed-schema-enum =
            context.cargoTest "actor_runtime_truth" "harness_kind_is_closed_schema_enum";
          terminal-fixture-endpoint-not-production-delivery = context.cargoTest
            "actor_runtime_truth"
            "fixture_human_endpoint_cannot_be_production_delivery";
          harness-daemon-applies-spawn-envelope-socket-mode =
            context.cargoTest "daemon" "harness_daemon_applies_spawn_envelope_socket_mode";
          harness-daemon-answers-status-readiness =
            context.cargoTest "daemon" "harness_daemon_answers_status_readiness";
          harness-daemon-returns-typed-unimplemented =
            context.cargoTest "daemon" "harness_daemon_returns_typed_unimplemented";
        }
      );

      apps = forSystems (
        system:
        {
          default = {
            type = "app";
            program = "${self.packages.${system}.default}/bin/persona-harness-daemon";
          };
        }
      );

      devShells = forSystems (
        system:
        let
          context = mkContext system;
        in
        {
          default = context.pkgs.mkShell {
            packages = [
              context.pkgs.jujutsu
              context.pkgs.pkg-config
              context.toolchain
            ];
          };
        }
      );

      formatter = forSystems (
        system:
        let
          context = mkContext system;
        in
        context.pkgs.nixfmt
      );
    };
}
