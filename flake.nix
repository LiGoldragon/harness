{
  description = "Interactive harness abstraction for Persona.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forSystems =
        function: nixpkgs.lib.genAttrs systems (system: function system nixpkgs.legacyPackages.${system});
      cargoLock = {
        lockFile = ./Cargo.lock;
        outputHashes = {
          "nota-codec-0.1.0" = "sha256-c32c6hzVP8pbuAWqKbD552nWSNS64CPSyMW23hrlUyg=";
          "nota-derive-0.1.0" = "sha256-2Gb50KBnqb1stlbCWcYvCRadO2VdMBb5a9limdyXx9I=";
          "persona-terminal-0.1.0" = "sha256-s9sAdnUBFumpiUAXtCrVAx+afOpcbvOV5tLcICLAA9o=";
          "terminal-cell-0.1.0" = "sha256-Aos+3HYumEOj6EOOGifAGYB0TQGA6TYVIyqitWYoVMY=";
          "signal-core-0.1.0" = "sha256-QGcKXD2ECbVrfOt1OWtkFoDFalV2/5rAYaKpBimjTPY=";
          "signal-persona-terminal-0.1.0" = "sha256-prv9VKZfz0a6Jq9mmPAXoamyzfMAvevX/KT3yzjtpzc=";
        };
      };
      mkHarnessPackage =
        pkgs: extraArgs:
        pkgs.rustPlatform.buildRustPackage (
          {
            pname = "persona-harness";
            version = "0.1.0";
            src = ./.;
            inherit cargoLock;
          }
          // extraArgs
        );
    in
    {
      packages = forSystems (
        system: pkgs: {
          default = mkHarnessPackage pkgs { };
        }
      );

      checks = forSystems (
        system: pkgs: {
          default = self.packages.${system}.default;
          harness-identity-projection-views = mkHarnessPackage pkgs {
            cargoTestFlags = [
              "--test"
              "smoke"
              "harness_identity_projection"
            ];
          };
          harness-identity-projection-source-constraint = mkHarnessPackage pkgs {
            cargoTestFlags = [
              "--test"
              "actor_runtime_truth"
              "harness_identity_projection_cannot_leak_everything_by_default"
            ];
          };
        }
      );

      devShells = forSystems (
        system: pkgs: {
          default = pkgs.mkShell {
            packages = [
              pkgs.cargo
              pkgs.clippy
              pkgs.rust-analyzer
              pkgs.rustc
              pkgs.rustfmt
            ];
          };
        }
      );

      formatter = forSystems (system: pkgs: pkgs.nixfmt);
    };
}
