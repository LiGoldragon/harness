{
  description = "Interactive harness abstraction for Persona.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";
  };

  outputs =
    { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system nixpkgs.legacyPackages.${system});
    in
    {
      packages = forSystems (
        system: pkgs:
        {
          default = pkgs.rustPlatform.buildRustPackage {
            pname = "persona-harness";
            version = "0.1.0";
            src = ./.;
            cargoLock = {
              lockFile = ./Cargo.lock;
              outputHashes = {
                "nota-codec-0.1.0" = "sha256-c32c6hzVP8pbuAWqKbD552nWSNS64CPSyMW23hrlUyg=";
                "nota-derive-0.1.0" = "sha256-2Gb50KBnqb1stlbCWcYvCRadO2VdMBb5a9limdyXx9I=";
                "persona-wezterm-0.1.0" = "sha256-QN9+D3+EKPMQPkXSXJRbov+Jt4x4Y2NQZ89lLiBSGMY=";
                "signal-core-0.1.0" = "sha256-AUot7j4Yf6rOJ1Rfa8cdqwu+WYPvoIMUxSdJRMZgH48=";
                "signal-persona-terminal-0.1.0" = "sha256-MY2MbtpduyMwtfxcIKwAToUVsxMJ6Cl6pylCBncYcOw=";
              };
            };
          };
        }
      );

      checks = forSystems (
        system: pkgs:
        {
          default = self.packages.${system}.default;
        }
      );

      devShells = forSystems (
        system: pkgs:
        {
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
