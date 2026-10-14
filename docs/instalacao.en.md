# Installation

There are four paths. This page covers the Windows installer and the Linux
packages; for the plain binary see [Quick start](inicio-rapido.md) and for
containers see [Docker and tests](docker.md).

## Windows — installer

Download `ConflatedPubSubSetup-<version>.exe` from the
[releases page](https://github.com/rspadim/redis-conflated-pubsub/releases). It
requires Windows 10 or newer and administrator rights, because it registers a
service.

The wizard opens with a menu of six options:

- configure Redis input → Redis output (the default)
- NATS input → NATS output
- Redis input → NATS output
- NATS input → Redis output
- use a `config.json` that already exists
- install the executable only, without configuring

The questions follow the choice: input and output pages (endpoint, database, the
name of the environment variable holding the password, subscription pattern or
subject, channel prefix and suffix) and a tuning page (conflation interval,
deduplication TTL, status HTTP port, log level). The output prefix already comes as
`conflated:` because the relay refuses a configuration where the input and the
output are the same Redis server with no prefix or suffix: its own output would
match its subscription.

Leaving the last page, the wizard builds the file and validates it with the relay's
own `--check-config`, in a draft in the temporary directory — before writing
anything to `Program Files`. If a file already exists at the destination it is kept
and validated as it is; if not, it is generated from the answers. The installer then
copies the files, writes `config.json` with absolute paths (a Windows service starts
with its working directory set to `System32`), registers the service with
restart-on-failure actions and, at the end, confirms whether the relay **actually
came up** — `sc start` alone would not prove that.

| Where | What |
| --- | --- |
| `C:\Program Files\Conflated Pub/Sub\` | executable, `config.json`, the examples and `scripts\` |
| `logs\` and `status.json` | inside that directory, through absolute paths in `config.json` |
| Start Menu | validate the configuration, edit `config.json`, documentation, uninstall |

Silent install, for a scripted deployment:

```powershell
.\ConflatedPubSubSetup-<version>.exe /VERYSILENT /CONFIG=C:\relay\config.json /SERVICE=1 /SERVICESTART=auto
```

| Parameter | Meaning |
| --- | --- |
| `/CONFIG=<file>` | Use a configuration that already exists. It must exist: otherwise the setup refuses to start and nothing is generated. |
| `/SERVICE=0\|1` | `1` (default) registers and starts the service; `0` only copies the files, for an operator who will finish by hand. |
| `/SERVICESTART=auto\|delayed-auto\|demand` | Startup type. Defaults to `auto`, or `demand` when the wizard's "start automatically" task is unchecked. |

Without `/CONFIG` the configuration is generated from the wizard's default answers
(Redis input to Redis output); to change those, run the wizard or edit
`config.json` afterwards.

An invalid value aborts before anything is written, and **nothing is shown** in
silent mode — a dialog would wait for a click that never comes. The non-zero exit
code is the signal, and `/LOG=<file>` keeps Inno's own log. Failures after the
files are copied (the relay rejecting the configuration, the service refusing to
start) abort the install the same way instead of showing a message box.

### Credentials

The relay reads passwords from environment variables, and a Windows service does
**not** see user variables: one set in your profile is invisible to the service,
which then exits at startup. The wizard solves it — the password field stores the
value in the service's **own environment**, an `Environment` entry under the
service key that the service manager injects into that process only, readable by
administrators alone. Leaving the field empty is supported, but the wizard warns
that the variable must already exist in that environment.

### Service account

The service runs as the passwordless virtual account
`NT SERVICE\redis-conflated-pubsub` instead of `LocalSystem`: it needs the network
and its own directory, nothing else. The installer grants write access to the
install directory only. If the machine refuses the virtual account, it falls back to
`LocalSystem` and says so on screen.

### Uninstalling

The uninstaller stops and removes the service and asks whether to keep
`config.json` and the logs.

## Linux — packages

Every release publishes deb, rpm, apk and Arch packages, built from the **static
musl** binary, so there are no runtime dependencies:

```sh
# Debian/Ubuntu
sudo dpkg -i redis-conflated-pubsub_<version>_amd64.deb
# Fedora/RHEL
sudo rpm -i redis-conflated-pubsub-<version>-1.x86_64.rpm
# Alpine
sudo apk add --allow-untrusted redis-conflated-pubsub_<version>_x86_64.apk
```

The package does **not** start the service on its own: it creates the service
account, reloads systemd and prints the command for you to enable it.

```sh
sudo systemctl enable --now redis-conflated-pubsub.service
```

| Path | What |
| --- | --- |
| `/usr/bin/redis-conflated-pubsub` | the binary |
| `/etc/redis-conflated-pubsub/config.json` | starting configuration; an upgrade never overwrites it |
| `/etc/redis-conflated-pubsub/redis.env` | secrets for the `*_env` variables, mode `0600`, read by systemd |
| `/usr/lib/systemd/system/redis-conflated-pubsub.service` | systemd unit (Debian, RPM, Arch) |
| `/usr/lib/sysusers.d/redis-conflated-pubsub.conf` | dedicated service account, created without needing `shadow-utils` |
| `/etc/init.d/redis-conflated-pubsub` | OpenRC script (Alpine, which has no systemd) |

The unit runs as the `redis-conflated-pubsub` user, keeps state in
`/var/lib/redis-conflated-pubsub`, points `EnvironmentFile` at the `redis.env`, uses
`Restart=on-failure` and is hardened (`NoNewPrivileges`, `PrivateTmp`,
`ProtectSystem=strict`, `ProtectHome`). Logs go to the journal.

## Container

The relay image and the single-container bundle with Redis/Valkey are in
[Docker and tests](docker.md).
