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
