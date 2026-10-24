{
  description = "atlas-doris - Atlas-compatible schema migration driver, differ, linter, executor, and external_schema plugin for Apache Doris 4.x and Apache Iceberg";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go_1_25
            buf
            protobuf
            protoc-gen-go
            protoc-gen-go-grpc
            gh
            gopls
            golangci-lint
            pre-commit
            lefthook
          ];

          shellHook = ''
            export GOWORK=off
            echo "atlas-doris dev shell"
            echo "  go:       $(go version)"
            echo "  buf:      $(buf --version)"
            echo "  protoc:   $(protoc --version)"
          '';
        };
      });
}
