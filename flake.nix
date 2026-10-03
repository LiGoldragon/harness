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
          cargoLibTest =
            testName:
            craneLib.cargoTest (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoTestExtraArgs = "--lib ${testName} -- --exact";
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
            cargoLibTest
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
          harness-daemon-applies-configured-socket-modes-and-owner-only-meta = context.cargoTest "daemon" "harness_daemon_applies_configured_socket_modes_and_owner_only_meta";
          harness-daemon-keeps-meta-and-supervision-on-separate-sockets = context.cargoTest "daemon" "harness_daemon_keeps_meta_and_supervision_on_separate_sockets";
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
          flow-id = context.cargoTest "flow_id" "codex_extracts_the_normalized_hex_23_to_29_candidate_and_prints_only_the_alias";
          flow-id-claude = context.cargoTest "flow_id" "claude_v5_claims_are_idempotent_private_and_separate_from_same_prefix_v4_claims";
          flow-id-claude-validation = context.cargoTest "flow_id" "claude_rejects_noncanonical_unsupported_version_and_invalid_variant_parent_sessions_without_claiming_a_lane";
          flow-id-claude-exhaustion = context.cargoTest "flow_id" "claude_fails_closed_when_every_eligible_literal_hex_candidate_is_occupied";
          flow-id-publication-race = context.cargoLibTest "flow_id::tests::claude_first_creator_publishes_complete_marker_only_after_the_stable_claim_lock";
          usage-claude-named-windows-and-listed-limits = context.cargoTest "usage_documents" "claude_named_windows_and_listed_limits_are_each_enumerated";
          usage-no-duration-inferred-from-reset = context.cargoTest "usage_documents" "a_listed_limit_keeps_its_reset_and_rate_without_an_inferred_duration";
          usage-claude-auxiliary-facts-named = context.cargoTest "usage_documents" "claude_auxiliary_allowance_and_spend_facts_are_named_not_windowed";
          usage-claude-unrecognized-and-unreadable-retained = context.cargoTest "usage_documents" "claude_unrecognized_windows_and_unreadable_percentages_are_retained";
          usage-codex-every-limit-window-and-fact = context.cargoTest "usage_documents" "codex_every_limit_window_and_source_fact_is_enumerated";
          usage-countdown-own-state = context.cargoTest "usage_documents" "countdown_is_its_own_state_for_pending_unknown_and_passed_resets";
          usage-reset-at-observation-passed = context.cargoTest "usage_documents" "a_reset_at_the_observation_second_has_passed_and_divides_nothing";
          usage-full-share-zero-rate = context.cargoTest "usage_documents" "a_full_share_has_no_remainder_and_a_zero_rate";
          usage-percent-domain = context.cargoTest "usage_documents" "percentages_outside_the_documented_domain_are_unreadable_not_clamped";
          usage-rate-rounding = context.cargoTest "usage_documents" "rates_round_toward_zero_and_the_weekly_uniform_rate_is_a_seventh";
          usage-elapsed-needs-fixed-period = context.cargoTest "usage_documents" "the_elapsed_position_needs_an_established_fixed_period";
          usage-local-reset-configured-zone = context.cargoTest "usage_documents" "the_local_reset_is_rendered_only_in_a_configured_zone";
          usage-claude-token-only-in-header = context.cargoTest "usage_sources" "claude_token_reaches_only_the_authorization_header";
          usage-claude-expired-token-never-sent = context.cargoTest "usage_sources" "expired_claude_token_is_reported_and_never_sent";
          usage-codex-same-account-homes-deduplicated = context.cargoTest "usage_sources" "codex_same_account_homes_are_one_subscription_and_failures_stay_per_home";
          usage-codex-thread-context = context.cargoTest "usage_sources" "codex_loaded_threads_report_bound_context_and_unbound_threads";
          usage-claude-session-context = context.cargoTest "usage_sources" "claude_live_sessions_report_transcript_proxy_and_its_states";
          usage-provider-failure-isolated = context.cargoTest "usage_sources" "one_read_carries_both_providers_when_one_fails";
          usage-codex-context-source-failures = context.cargoTest "usage_sources" "every_attempted_codex_context_source_reports_its_failure";
          usage-claude-registry-failures = context.cargoTest "usage_sources" "an_absent_or_unreadable_claude_registry_is_reported_not_skipped";
          usage-collector-failed = context.cargoTest "usage_sources" "a_reader_that_cannot_run_reports_every_collector_failed";
          usage-daemon-scope-without-instances = context.cargoTest "usage_daemon" "the_daemon_answers_the_usage_query_without_any_configured_instance";
          usage-both-clients-one-call = context.cargoTest "usage_daemon" "both_clients_print_the_snapshot_in_one_call";
          usage-user-service-launcher = context.cargoTest "usage_daemon" "the_user_service_launcher_writes_its_typed_configuration_and_becomes_the_daemon";
          usage-launcher-refusals = context.cargoTest "usage_daemon" "the_launcher_refuses_without_a_service_runtime_directory_or_with_an_argument";
          usage-cli-human-view = context.cargoTest "component_cli" "usage_cli_leads_each_window_with_remaining_time_left_reset_and_rate";
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
        usage = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/harness-usage";
        };
        flow-id = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/flow-id";
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
