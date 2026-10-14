# Linux packages (nfpm.yaml)

The Linux packages published with each release are described by
[`../nfpm.yaml`](../nfpm.yaml) and the files in this directory. They are built
from the same static `x86_64-unknown-linux-musl` binary that the release
archives ship, so they declare no dependencies and one package per family covers
every supported release.

| File | Packagers | Installed as |
| --- | --- | --- |
| `config.json` | all | `/etc/redis-conflated-pubsub/config.json` (config file, `0644`) |
| `redis.env` | all | `/etc/redis-conflated-pubsub/redis.env` (config file, `0600`, root-only; holds the `*_env` secrets) |
| `systemd/redis-conflated-pubsub.service` | deb, rpm, archlinux | `/usr/lib/systemd/system/redis-conflated-pubsub.service` |
| `sysusers/redis-conflated-pubsub.conf` | deb, rpm, archlinux | `/usr/lib/sysusers.d/redis-conflated-pubsub.conf` (declares the service account) |
| `openrc/redis-conflated-pubsub` | apk | `/etc/init.d/redis-conflated-pubsub` (Alpine has no systemd) |
| `scripts/postinstall.sh` | deb, rpm, archlinux | package `postinst` / `%post` / `post_install()`: creates the account, reloads systemd |
| `scripts/preremove.sh` | deb, rpm, archlinux | package `prerm` / `%preun` / `pre_remove()`: stops and disables the service on removal, not on upgrade |
| `scripts/postremove.sh` | deb, rpm, archlinux | package `postrm` / `%postun` / `post_remove()`: reloads systemd; a Debian `purge` also removes the account, the state and the configuration |
| `apk/post-install.sh` | apk | `.post-install`: creates the account with busybox `adduser`, creates the runtime directories |
| `apk/pre-deinstall.sh` | apk | `.pre-deinstall`: stops and removes the service from the default runlevel |

Notes on the layout:

- The service account is created by `systemd-sysusers` when it exists, and the
  `sysusers.d` drop-in also lets systemd create it at boot if the post-install
  script never ran. `useradd`/`groupadd` are only a fallback, for hosts without
  systemd. This is why the RPM package has no `shadow-utils` dependency, which
  minimal images do not ship.
- Alpine gets no systemd unit and no `sysusers.d` drop-in; its post-install
  script also creates `/var/lib/redis-conflated-pubsub` and
  `/var/log/redis-conflated-pubsub`, because OpenRC has no `StateDirectory` or
  `LogsDirectory`.
- nFPM wraps the shell scripts as `post_install()`, `pre_remove()` and
  `post_remove()` functions in the Arch `.INSTALL` file, as pacman expects.
- Installation never enables or starts the service, because the shipped
  configuration points at `localhost` and must be edited first.
- The packages are unsigned, like the release archives.
- Keep `packaging/**` shell scripts on LF line endings (see `../.gitattributes`):
  a CRLF shebang makes the maintainer scripts fail, and CRLF leaks into the
  `EnvironmentFile` values on the systemd side.

## Building and testing locally

From the repository root, with the musl binary placed at
`dist/redis-conflated-pubsub`:

```sh
for packager in deb rpm apk archlinux; do
  VERSION=0.3.1 nfpm package --config nfpm.yaml --packager "$packager" --target dist/
done
```

`VERSION` is required, holds the bare version (no leading `v`) and is the only
value that changes between releases; the release workflow reads it from
`Cargo.toml`. The workflows build the packages and install them on every CI run
and on every tag — Ubuntu 20.04, 22.04, 24.04 and 26.04, plus Fedora, Alpine and
Arch — so a broken package layout fails before a release is published. To repeat
an install check by hand:

```sh
docker run --rm -v "$PWD/dist:/dist:ro" ubuntu:24.04 bash -c '
  apt-get update -qq && apt-get install -y -qq /dist/redis-conflated-pubsub_*_amd64.deb
  redis-conflated-pubsub --config /etc/redis-conflated-pubsub/config.json --check-config
  dpkg --purge redis-conflated-pubsub'
```

## Known limitation: name resolution

Because the binary is statically linked against musl, it resolves names with the
musl resolver, which reads `/etc/resolv.conf` and `/etc/hosts` and ignores
`/etc/nsswitch.conf`. Pointing Redis or NATS at a hostname that systemd-resolved
resolves through `nss-resolve` (split-DNS/`resolvectl` per-link domains) or
through mDNS (`.local`) therefore fails, even though `getent hosts` works on the
same machine. Use an IP address, `/etc/hosts`, or a name served by the
configured DNS servers in `/etc/resolv.conf`.
