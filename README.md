# dkol-server

An implementation of the backend portion of ["Dokapon the World
Online"](https://www.sting.co.jp/dokapon/world/index.htm)
(ドカポン・ザ・ワールド Online)

## Build and run

```bash
nix build ./result/bin/dkol-server --addr <IP>
```

A self-signed cert for dkol.sting.jp is generated into --data on first run; the
client accepts an untrusted root as long as the name matches, so this works for
unmodified clients.

Serverside, everything persistent is stored in --data. Accounts are issued by
the admin.

```bash
# Create a user
dkol-server --data <dir> adduser <handle> <password>
# Reset password
dkol-server --data <dir> passwd <account id> <new password>
```

The game is saved once per in-game day. The host can continue from the room's
セーブから button, and only saved players can join, each in their own seat.

State is in /var/lib/dkol-server, run as user dkol.

## Pointing a client at it

The client looks up dkol1st.sting.jp and falls back to 210.196.202.18. Make that
name resolve to the server or use a client patched with a different name.
Probably with the Hosts file on your computer is the easiest way.

## License

MIT License (available in the source tree as /LICENSE)
