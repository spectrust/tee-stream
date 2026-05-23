{
  description = "tee-stream";

  nixConfig = {
    extra-substituters = [
      "https://nix-community.cachix.org"
      "https://fenix.cachix.org"
    ];
    extra-trusted-public-keys = [
      "nix-community.cachix.org-1:mB9FSh9qf2dCimDSUo8Zy7bkq5CX+/rkCWyvRCYg3Fs="
      "fenix.cachix.org-1:ecJhr+RdYEdcVgUkjruiYhjbBloIEGov7bos90cZi0Q="
    ];
  };

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    # Provides some nice helpers for multiple system compatibility
    flake-utils.url = "github:numtide/flake-utils";
    # Provides rust and friends
    fenix = {
      url = "github:nix-community/fenix/monthly";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      fenix,
    }:
    # Calls the provided function for each "default system", which
    # is the standard set.
    flake-utils.lib.eachDefaultSystem (
      system:
      # instantiate the package set for the supported system, with our
      # rust overlay
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ fenix.overlays.default ];
        };
      in
      {
        devShells = {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
                cargo
                cargo-edit
                rustfmt
                coreutils
                direnv
                rust-analyzer
                rustc
              ];
          };
        };
      }
    );
}
