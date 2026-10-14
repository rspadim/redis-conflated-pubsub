# Windows installer

Inno Setup 6 script that installs the relay as a Windows service. Build it from
the repository root, with the release executable already built:

```powershell
cargo build --release --locked
tools\make-icons.ps1                                              # only after changing the mark
& "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" "/DAppVersion=0.3.1" "installer\redis-conflated-pubsub.iss"
.\installer\output\ConflatedPubSubSetup-0.3.1.exe
```

`AppVersion` must be numeric (`a.b.c` or `a.b.c.d`): it feeds the file's fixed
version resource, which Windows comparisons and the resource checks read. CI
passes the release tag without the leading `v`; a local build defaults to `0.0.0`.

Silent mode, for a scripted deployment:

```powershell
.\ConflatedPubSubSetup-0.3.1.exe /VERYSILENT /CONFIG=C:\relay\config.json /SERVICE=1 /SERVICESTART=auto
```

| Parameter | Meaning |
| --- | --- |
| `/CONFIG=<file>` | Use a configuration that already exists. It must exist: the setup refuses to start otherwise, and nothing is generated. |
| `/SERVICE=0\|1` | `1` (the silent default) registers and starts the service; `0` only drops the files, for an operator who will finish by hand. The wizard asks through its tasks instead — note that Inno leaves a `checkedonce` task unchecked on a later installation. |
| `/SERVICESTART=auto\|delayed-auto\|demand` | Startup type; `auto` is the silent default. In the wizard it is `auto` unless the "start automatically" task is unchecked. |

Without `/CONFIG` the configuration is generated from the wizard's default answers
(Redis input to Redis output); to change those, run the wizard or edit
`config.json` afterwards.

An invalid value aborts before anything is written, and **nothing is shown** in
silent mode — a dialog would wait for a click that never comes. `/LOG=<file>`
keeps Inno's own log.

### What a scripted install can check

| Exit code | Meaning |
| --- | --- |
| `0` | Everything the setup was asked to do was done. |
| `1` | The parameters were rejected, or `/CONFIG` does not exist: nothing was written. |
| `9` | A step **after** the files were copied failed. Inno returns `0` for a script failure (verified: `RaiseException` and `Abort()` from `ssPostInstall` both leave the exit code at `0`), so the setup reports this one through `GetCustomSetupExitCode`. |
| `5`, `7`, `8` | Inno's own codes: cancelled, could not proceed, restart needed. |

`{app}\install-report.txt` says which step failed, one line per step:

```
configuration: C:\Program Files\Conflated Pub/Sub\config.json
configuration: generated
configuration: accepted by the relay
service: redis-conflated-pubsub (auto)
service: installed
service: started
relay: running
result: ok
```

A silent uninstall keeps `config.json`: nobody is there to answer, and deleting it
is the destructive answer.

## What the wizard does

1. **Input, output and tuning pages** (after the tasks page). The wizard asks the
   questions instead of handing over a JSON file:
   - input: Redis endpoint (`host:port`, or `rediss://host:6380` for TLS),
     database, the name of the environment variable holding the password, and the
     subscription pattern;
   - output: the same endpoint fields plus the channel prefix and suffix. The
     prefix defaults to `conflated:` because the relay refuses a configuration
     where the input and the output are the same Redis server with no prefix or
     suffix: its own output would match its subscription.
   - tuning: conflation interval, deduplication TTL, status HTTP port and log
     level. Zero disables conflation, deduplication or the status listener.
2. **Destination page.** `config.json` in the install directory by default. An
   existing file is kept and used as it is, so an upgrade never overwrites the
   operator's configuration. Leaving the page validates the outcome: if the file
   does not exist yet, the wizard builds a draft in the temporary directory with
   the same content and the same absolute paths, and the relay's own
   `--check-config` judges it — the echo-loop and backend rules are reported here,
   before anything is installed.
3. **Files.** The executable, both example configurations, the README, the
   licence, the generated JSON Schema and the helper scripts.
4. **Service.** Optional task: registers `redis-conflated-pubsub` with auto or
   manual start, sets a description and restart-on-failure actions, then starts it.
5. **Verification.** `sc start` only means the service manager accepted the
   request, so `verify-service.ps1` asks the relay whether it came up, and the
   finish page reports the answer.

The uninstaller stops and removes the service and asks whether to keep
`config.json`, the lock file and the logs.

Only Redis endpoints are offered on both sides. A Core NATS topology, or anything
beyond one subscription and one output, is configured by pointing the destination
page at a hand-written file; `config.nats.example.json` and the project site cover
those cases.

## Why the paths are rewritten

The relay resolves `logging.directory`, `instance_lock.path` and `status.path`
against its working directory. A Windows service starts with `System32` as its
working directory, so an installed configuration copied verbatim from the example
would write its logs and its lock file there. `install-config.ps1` rewrites those
three fields to absolute paths under the install directory.

It also writes UTF-8 **without a BOM**: `Set-Content -Encoding UTF8` in Windows
PowerShell 5.1 prepends one, and `serde_json` rejects a BOM, so a configuration
written that way would fail to load.

## Language files

The wizard ships the same ten languages as the sibling RedisExcel project:
English (default), Brazilian Portuguese, Spanish, Chinese Simplified, Chinese
Traditional, Russian, French, Korean, German and Japanese. The standard Inno UI is
fully translated by the official `.isl` files; only the messages this wizard adds
need translating.

Each language reads two message files, in this order:

```ini
Name: "japanese"; MessagesFile: "compiler:Languages\Japanese.isl,languages\messages.english.isl,languages\messages.japanese.isl"
```

The last file wins, and `messages.english.isl` holds the base wording of every
key. So a language file defines only what it translates and the rest falls back to
English — no language can ever show a raw `{cm:...}` constant, and a language with
no file at all still works. Nothing is defined inline in the `.iss` on purpose: an
entry in the main script applies to every language and would shadow these files.

To add or extend a language:

1. Copy `languages\messages.english.isl`, translate the values and keep the keys.
   Save as UTF-8 **without BOM** and keep the `%1`/`%n` placeholders intact.
2. Add the `.isl` to the language's `MessagesFile` entry in the `.iss`.
3. Compile and check the language in the selector.

A language file does not have to cover every key. The configuration-wizard keys
were added after the first translation pass, so the seven languages other than
English, Brazilian Portuguese and Spanish currently fall back to English for those
labels; the older installer messages are translated in all ten.

## Helper scripts

| Script | Used by | Purpose |
| --- | --- | --- |
| `validate-config.ps1` | wizard, `Validate-Config.cmd` | Runs `--check-config` and reports the relay's own output. |
| `New-Config.ps1` | wizard | Turns the wizard answers into `config.json` with absolute paths, keeping an existing file unless `-Force` is given. |
| `service.ps1` | wizard, uninstaller | Install, uninstall, start, stop, restart or query the service. |
| `verify-service.ps1` | wizard (post-install) | Checks that the relay actually started, not only that the service was started. |
| `Validate-Config.cmd` | Start Menu shortcut | Double-click wrapper around `validate-config.ps1`. |

Every script is idempotent and reports through its exit code, which is what the
wizard reacts to. `New-Config.ps1` can also be run by hand to regenerate a
configuration: it accepts the same answers as parameters.

## Credentials

The relay reads credentials from environment variables (`password_env`,
`token_env`), and a Windows service does **not** see the user variables of
whoever installed it: a variable set in the operator's profile is invisible to the
service, which then exits at startup.

The wizard therefore asks for the secret value on the same page that asks for the
variable name, and stores it in the service's **own** environment block: a
`REG_MULTI_SZ` value named `Environment` under the service's registry key, which
the service control manager injects into that service's process and nothing else.
The secret stays readable only by administrators.

- Leaving the value empty is supported: the wizard then warns that the variable
  must already exist in the service environment, because the relay will exit at
  startup until it does.
- Type the value as ASCII if you can. `SaveStringToFile` is a byte-oriented API,
  so a password with accents may not survive the temporary file the installer
  writes. Setting the variable yourself avoids the question entirely.
- The temporary file holding the values is deleted right after the write, and the
  values never reach a command line.

`Set-ServiceSecrets.ps1` does the work and can also be run by hand:

```powershell
# what is already there, and what is missing
.\scripts\Set-ServiceSecrets.ps1 -Verify -ServiceName redis-conflated-pubsub -Names 'REDIS_PASSWORD,NATS_TOKEN'

# store a value
'set REDIS_PASSWORD=secret' | Set-Content secrets.txt
.\scripts\Set-ServiceSecrets.ps1 -ServiceName redis-conflated-pubsub -SecretFile secrets.txt
Remove-Item secrets.txt
```

It merges instead of replacing, so variables added by hand survive a reinstall.

## The Windows service

`redis-conflated-pubsub.exe` implements the service protocol, so the registered
service behaves like any other: `sc start` reports success only once the relay has
answered, `sc stop` drains the queues, and a crash triggers the restart-on-failure
actions the installer sets. The setup still checks the relay's own status file
afterwards, because a service that started and died a second later would otherwise
look like a success.

## The service account

The service runs under the passwordless virtual account
`NT SERVICE\redis-conflated-pubsub`, not LocalSystem: the relay needs the network
and its own directory, nothing else. The installer grants that account write
access to the install directory (logs, lock file, status snapshot) and leaves
LocalSystem only if the machine refuses the account, saying so in the wizard.

Two details worth knowing:

- `icacls` cannot resolve a virtual account before the service control manager has
  registered it — it fails with error 1332, "no mapping between account names and
  security IDs". The installer therefore grants the access and, when that fails,
  starts the service once (which registers the account), grants, and lets the
  wizard start it again.
- The uninstaller stops and deletes the service, then removes the access entry
  from the directory, so a kept install directory does not carry a grant for an
  account that will not exist again.

## Upgrades

`PrepareToInstall` stops an existing service before the files are copied. Windows
allows a running image to be renamed but not overwritten, so without that step the
copy would be deferred to the next reboot and the service would keep executing the
old binary while the wizard reported success. If the service task is unchecked
during an upgrade, a service that was stopped is started again.
