{
  description = "dkol-server: Dokapon the World online service";

  inputs.nixpkgs.url = "nixpkgs";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      src = pkgs.lib.fileset.toSource {
        root = ./.;
        fileset = pkgs.lib.fileset.unions [ ./Cargo.toml ./Cargo.lock ./src ];
      };
      build = p: p.rustPlatform.buildRustPackage {
        pname = "dkol-server";
        version = "0.1.1";
        inherit src;
        cargoLock.lockFile = ./Cargo.lock;
        nativeBuildInputs = [ p.buildPackages.pkg-config ];
        buildInputs = [ p.openssl ];
      };
      dkol-server = build pkgs;
      static = build pkgs.pkgsStatic;
    in {
      packages.${system} = {
        default = dkol-server;
        image = pkgs.dockerTools.buildLayeredImage {
          name = "ghcr.io/wesl-ee/dkol-server";
          tag = static.version;
          config = {
            Entrypoint = [ "${static}/bin/dkol-server" "--data" "/data" ];
            Volumes."/data" = { };
          };
        };
      };

      nixosModules.default = { config, lib, ... }:
        let
          cfg = config.services.dkol-server;
          args = [ "--addr" cfg.address "--lobbies" (toString cfg.lobbies) "--data" "/var/lib/dkol-server" ]
            ++ lib.optionals (cfg.motd != "") [ "--motd" cfg.motd ];
        in {
          options.services.dkol-server = with lib; {
            enable = mkEnableOption "the Dokapon the World server";
            package = mkOption {
              type = types.package;
              default = dkol-server;
            };
            address = mkOption {
              type = types.str;
              description = "IPv4 address advertised to clients in the server list";
            };
            lobbies = mkOption {
              type = types.ints.between 1 6;
              default = 2;
            };
            motd = mkOption {
              type = types.str;
              default = "";
              description = "text shown in the client's info window on connect";
            };
            logLevel = mkOption {
              type = types.str;
              default = "info";
              description = "RUST_LOG filter";
            };
            openFirewall = mkOption {
              type = types.bool;
              default = false;
              description = "open the first, login and lobby TCP ports";
            };
          };

          config = lib.mkIf cfg.enable {
            users.users.dkol = {
              isSystemUser = true;
              group = "dkol";
            };
            users.groups.dkol = { };
            environment.systemPackages = [ cfg.package ];
            systemd.services.dkol-server = {
              wantedBy = [ "multi-user.target" ];
              after = [ "network-online.target" ];
              wants = [ "network-online.target" ];
              environment.RUST_LOG = cfg.logLevel;
              serviceConfig = {
                ExecStart = "${cfg.package}/bin/dkol-server ${lib.escapeShellArgs args}";
                User = "dkol";
                Group = "dkol";
                StateDirectory = "dkol-server";
                Restart = "on-failure";
                NoNewPrivileges = true;
                ProtectSystem = "strict";
                ProtectHome = true;
                PrivateTmp = true;
              };
            };
            networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall
              ([ 19999 20001 ] ++ lib.genList (n: 20003 + n) cfg.lobbies);
          };
        };

      checks.${system} = {
        build = dkol-server;
        clippy = dkol-server.overrideAttrs (o: {
          pname = "dkol-server-clippy";
          nativeBuildInputs = o.nativeBuildInputs ++ [ pkgs.clippy ];
          buildPhase = "cargo clippy --offline --all-targets -- -D warnings";
          installPhase = "touch $out";
          doCheck = false;
        });
        fmt = pkgs.runCommand "dkol-server-fmt" { nativeBuildInputs = [ pkgs.cargo pkgs.rustfmt ]; } ''
          cd ${src} && cargo fmt --check && touch $out
        '';
      };

      devShells.${system}.default = pkgs.mkShell {
        inputsFrom = [ dkol-server ];
        packages = with pkgs; [ cargo rustc clippy rustfmt rust-analyzer wireshark-cli ilspycmd ];
      };
    };
}
