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
          schemaFilter =
            path: type:
            (type == "regular" || type == "directory") && (builtins.match ".*/schema(/.*)?" path != null);
          testFilter =
            path: type:
            (type == "regular" || type == "directory") && (builtins.match ".*/tests(/.*)?" path != null);
          sourceFilter =
            path: type:
            (craneLib.filterCargoSources path type) || (schemaFilter path type) || (testFilter path type);
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter = sourceFilter;
            name = "source";
          };
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
              pname = "harness";
              meta.mainProgram = "harness";
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
          harness-identity-projection-views = context.cargoTest "smoke" "harness_identity_projection_keeps_full_owner_view";
          harness-identity-projection-source-constraint = context.cargoTest "actor_runtime_truth" "harness_identity_projection_cannot_leak_everything_by_default";
          harness-kind-closed-schema-enum = context.cargoTest "actor_runtime_truth" "harness_kind_is_closed_schema_enum";
          harness-kind-includes-all-four-variants = context.cargoTest "actor_runtime_truth" "harness_kind_includes_all_four_variants";
          harness-kind-has-no-command-line-argument-projection = context.cargoTest "actor_runtime_truth" "harness_kind_has_no_command_line_argument_projection";
          harness-daemon-accepts-fixture-kind-from-single-binary-configuration-argument = context.cargoTest "daemon" "harness_daemon_accepts_fixture_kind_from_single_binary_configuration_argument";
          harness-daemon-accepts-codex-kind-from-single-binary-configuration-argument = context.cargoTest "daemon" "harness_daemon_accepts_codex_kind_from_single_binary_configuration_argument";
          harness-daemon-configuration-rejects-multiple-arguments = context.cargoTest "daemon" "harness_daemon_configuration_rejects_multiple_arguments";
          terminal-fixture-endpoint-not-production-delivery = context.cargoTest "actor_runtime_truth" "fixture_human_endpoint_cannot_be_production_delivery";
          harness-daemon-binds-working-socket-with-configured-mode = context.cargoTest "daemon" "harness_daemon_binds_working_socket_with_configured_mode";
          harness-daemon-applies-configured-working-socket-mode-and-owner-only-supervision = context.cargoTest "daemon" "harness_daemon_applies_configured_working_socket_mode_and_owner_only_supervision";
          harness-daemon-answers-status-readiness = context.cargoTest "daemon" "harness_daemon_answers_status_readiness";
          harness-daemon-delivers-message-to-terminal-endpoint = context.cargoTest "daemon" "harness_daemon_delivers_message_to_terminal_endpoint";
          harness-daemon-rejects-message-delivery-without-terminal-endpoint = context.cargoTest "daemon" "harness_daemon_rejects_message_delivery_without_terminal_endpoint";
          harness-daemon-answers-component-supervision-relation = context.cargoTest "daemon" "harness_daemon_answers_component_supervision_relation";
          harness-daemon-answers-meta-harness-relation = context.cargoTest "daemon" "harness_daemon_answers_meta_harness_relation_with_typed_unimplemented";
          harness-daemon-resolves-exact-pi-model-request = context.cargoTest "daemon" "harness_daemon_resolves_exact_pi_model_request";
          harness-daemon-resolves-capability-profile-request = context.cargoTest "daemon" "harness_daemon_resolves_capability_profile_request";
          harness-daemon-returns-typed-model-unavailable-reasons = context.cargoTest "daemon" "harness_daemon_returns_typed_model_unavailable_reasons";
          harness-daemon-validates-continuation-handles-at-harness-boundary = context.cargoTest "daemon" "harness_daemon_validates_continuation_handles_at_harness_boundary";
          harness-daemon-reports-adapter-configuration-missing-for-unlaunchable-match = context.cargoTest "daemon" "harness_daemon_reports_adapter_configuration_missing_for_unlaunchable_match";
          harness-daemon-watch-transcript-returns-typed-snapshot = context.cargoTest "daemon" "harness_daemon_watch_transcript_returns_typed_snapshot";
          harness-daemon-unwatch-transcript-returns-final-retraction-ack-on-subscribed-stream = context.cargoTest "daemon" "harness_daemon_unwatch_transcript_returns_final_retraction_ack_on_subscribed_stream";
          harness-daemon-watch-transcript-stream-delivers-published-observation-and-final-ack = context.cargoTest "daemon" "harness_daemon_watch_transcript_stream_delivers_published_observation_and_final_ack";
          harness-observed-turn-projects-assistant-text-and-defers-accumulated-context = context.cargoTest "claude_session_observation" "observed_turn_projects_assistant_text_and_defers_accumulated_context";
          harness-claude-session-observation-is-pushed-to-subscriber-without-polling = context.cargoTest "claude_session_stream" "claude_session_observation_is_pushed_to_subscriber_without_polling";
          harness-daemon-allows-nested-watchers-for-same-harness-without-cross-closing = context.cargoTest "daemon" "harness_daemon_allows_nested_watchers_for_same_harness_without_cross_closing";
          harness-daemon-rejects-cross-harness-nested-watch-without-leaking-subscription = context.cargoTest "daemon" "harness_daemon_rejects_cross_harness_nested_watch_without_leaking_subscription";
          harness-daemon-returns-typed-unimplemented = context.cargoTest "daemon" "harness_daemon_returns_typed_unimplemented";
          harness-cli-reaches-working-socket = context.cargoTest "component_cli" "harness_cli_reaches_working_socket_and_prints_typed_reply";
          meta-harness-cli-reaches-policy-socket = context.cargoTest "component_cli" "meta_harness_cli_reaches_policy_socket_and_prints_typed_reply";
        }
      );

      apps = forSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/harness";
        };
        daemon = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/harness-daemon";
        };
        meta = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/meta-harness";
        };
      });

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
