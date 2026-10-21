# Conflated Pub/Sub — repository rules

These apply together with the global ergonomic practices. They are decisions
already made in this project: do not reopen them, follow them.

## 1. The payload is byte-transparent

The relay does not type, parse, schema-validate or convert payloads. The type is
`Arc<[u8]>` (`src/service/backend.rs`, `InputEnvelope`), commented in the code as
"Raw payload bytes"; `src/DESIGN.md` states that the payload "is never rewritten,
so the relay stays byte-transparent". The only two UTF-8 conversions in `src/` are
the HTTP endpoint's request line and one metadata line — neither touches a message
payload.

Consequences:

- **Do not propose** content typing, payload schemas, configurable encoding or
  format validation. That is not the relay's job.
- What gets validated is the **address** (channel, subject, glob, transport family,
  exactly one backend) and the **transport limit** (payload ceiling, oversized
  policy). Nothing else.
- When a backend accepts a narrower domain than "arbitrary bytes" — PostgreSQL's
  `text`, which refuses NUL and requires a valid encoding, for example — that is
  the transport's business, not the relay's. The payload passes through when it
  fits, and what does not fit lands in the drop counters that already exist. It is
  the same reasoning that already drops NATS headers for making the wire size
  variable.

## 2. Consume and write, do not invent in the user's environment

Against the services the relay uses (Redis, NATS, PostgreSQL) it only **consumes**
and **writes**:

- No DDL: it does not create tables, triggers, functions, extensions or indexes.
- No conventions that require new structure in the user's database (a channel
  registry table, a signal channel, a control table).
- No speculative knobs: if the case does not exist today, do not add configuration
  for it. Adding it later is cheap.

## 3. Reuse what exists before inventing

Before proposing a new mechanism, look for the equivalent in the project and reuse
it with the constant or limit of the new case. Precedents worth following:

- `validate.rs` already rejects an empty subscription list → the same rule applies
  to a subscription that would listen to no channel at all.
- The preflight against the NATS server-advertised `max_payload` is the precedent
  for any payload ceiling of another backend.
- The service's own environment block (installer) is the credential mechanism; do
  not invent another one for a new backend.

## Shell for agents

This is a **Rust** project, but the tooling around it is Windows-first (Inno
Setup, PowerShell helpers), and all of it is reachable from PowerShell:

- **PowerShell is available and preferred over bash on Windows.** Build with
  `cargo build --release --locked`, test with `cargo test`, compile the
  installer with `& "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
  "/DAppVersion=<ver>" "installer\redis-conflated-pubsub.iss"`. Under WSL the
  POSIX tools are fine for the Rust side and for Redis/NATS fixtures.
- **When a bash-style command is unavoidable, prefer the PowerShell equivalent**
  on the Windows side: `grep` -> `Select-String`, `sed`/`awk` -> `-replace`/
  `-split`, `head`/`tail` -> `Select-Object -First/-Last`, `which` ->
  `Get-Command`.
- **Quoting differs between the two shells** and silently corrupts output: a
  bash heredoc (`<<'EOF'`) and `$(...)` do not work in PowerShell (`<<` is
  reserved, `$(...)` is subexpression syntax). Write the body to a file and pass
  `-File` instead of inlining a long multi-line command.
- **Never rewrite a text file with `Set-Content`/`Out-File` just to change a few
  bytes.** Both re-encode the file; this family of projects already had a
  mixed ASCII/CJK/accented `.iss` corrupted that way. Use a targeted replace, or
  `[IO.File]::WriteAllText($p, $text, (New-Object Text.UTF8Encoding($false)))`,
  and verify with a strict decode (`New-Object Text.UTF8Encoding($false, $true)`).
  The console showing `?` does NOT mean the file is broken; a failed strict
  decode does.
- **Inno Setup parsing traps** that look like code errors: a continuation line
  that BEGINS with `[` is read as a section tag ("Invalid section tag") - keep a
  bracketed argument on the line that opens the call; and `const`/`type`/`var`
  belong in the declarations block at the top of `[Code]`, not between
  procedures.
- **A wizard built with `PrivilegesRequired=admin` runs elevated**, and an
  unelevated process can neither kill it (`Stop-Process` -> "Acesso negado") nor
  capture its window (`PrintWindow` returns black). To validate layout, compile
  a throwaway copy with `PrivilegesRequired=lowest` into a temp directory, and
  never leave that copy in `installer\output`.
- **This wizard runs its helpers with `SW_HIDE`** (`RunScript`), so helper
  console output is invisible during a normal install: a console banner there
  would be dead weight. User-visible text belongs in the `.isl` files.
- **Endpoints are probed by the relay's own client.** The wizard's "Test
  connection" button runs `redis-conflated-pubsub --test-connection` against a
  draft of the configuration it would write (`ProbeEndpoints` in the `.iss`).
  That flag validates the configuration, then connects to every backend with the
  runtime's own client configuration (Redis: connect + `PING`; NATS: the full
  connection plan), prints one line per backend and exits non-zero when any of
  them failed. It is read-only. Do not replace it with a PowerShell/socket
  probe: reachability says nothing about credentials, TLS or a connection-string
  the client rejects, and those are the cases that actually fail. Also do not
  make `--check-config` connect - it is documented and tested as a pure
  validation that returns before dialling.
