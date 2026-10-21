//! Pins the rule that the installer's connection probe is READ-ONLY.
//!
//! The wizard probes a server the user typed in, which often belongs to
//! somebody else. A probe may read, must never write or publish, and its verdict
//! is "connected and authenticated" - not "allowed to run this command".
//!
//! Two layers are asserted so the rule cannot regress silently:
//!
//! * the ladder in [`super::REDIS_PROBE_COMMANDS`] is checked command by command
//!   against a read-only allow-list, and the classifier that decides whether an
//!   error means "not connected" is exercised against `MISCONF`/`NOPERM`/auth
//!   failures;
//! * the probe sources (`src/main.rs`, `src/service/nats/connect.rs`) and the
//!   installer are scanned: every `redis::cmd` in the probe, and every backend
//!   call in the NATS connect module, has to be on the allow-list, and no
//!   installer helper may publish, create a key/stream, or call a CLI.

use super::{REDIS_PROBE_COMMANDS, is_auth_failure, is_server_reply};

/// Commands a probe may send. Reads of data or server metadata only: no `SET`,
/// `DEL`, `INCR`, `LPUSH`, `HSET`, `SADD`, `EXPIRE`, no `PUBLISH`, no
/// `SUBSCRIBE`/`PSUBSCRIBE` (the relay's own job, and it changes server state),
/// no `FLUSH*`. `CLIENT` is connection metadata (`SETINFO`/`SETNAME`), which the
/// Redis client sends on its own and which carries no user data.
const READ_ONLY_COMMANDS: &[&str] = &[
    "PING", "DBSIZE", "GET", "MGET", "EXISTS", "TYPE", "TTL", "PTTL", "SCAN", "INFO", "PUBSUB",
    "CLIENT", "HELLO",
];

/// Words that may never appear as the first token of a probe command.
const FORBIDDEN_COMMANDS: &[&str] = &[
    "SET",
    "SETEX",
    "PSETEX",
    "SETNX",
    "GETSET",
    "GETDEL",
    "MSET",
    "DEL",
    "UNLINK",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "APPEND",
    "LPUSH",
    "RPUSH",
    "LPOP",
    "RPOP",
    "HSET",
    "HMSET",
    "HDEL",
    "SADD",
    "SREM",
    "ZADD",
    "EXPIRE",
    "PEXPIRE",
    "PERSIST",
    "RENAME",
    "MOVE",
    "COPY",
    "PUBLISH",
    "SPUBLISH",
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "SSUBSCRIBE",
    "XADD",
    "FLUSHALL",
    "FLUSHDB",
    "RESTORE",
    "EVAL",
    "EVALSHA",
    "FUNCTION",
    "CONFIG",
    "SCRIPT",
    "MIGRATE",
];

#[test]
fn redis_probe_ladder_is_read_only() {
    assert!(
        !REDIS_PROBE_COMMANDS.is_empty(),
        "the probe must send at least one command"
    );
    for command in REDIS_PROBE_COMMANDS {
        let name = command[0].to_ascii_uppercase();
        assert!(
            READ_ONLY_COMMANDS.contains(&name.as_str()),
            "probe command '{name}' is not on the read-only allow-list"
        );
        assert!(
            !FORBIDDEN_COMMANDS.contains(&name.as_str()),
            "probe command '{name}' mutates the server"
        );
    }
    // The MISCONF regression: the probe must not depend on PING, which a server
    // with `stop-writes-on-bgsave-error yes` answers with an error while serving
    // every read. A read has to be tried first.
    assert_ne!(
        REDIS_PROBE_COMMANDS[0][0].to_ascii_uppercase(),
        "PING",
        "the first probe command must be a read, not PING"
    );
}

#[test]
fn every_redis_command_in_the_probe_entry_point_is_a_read() {
    let source = include_str!("../../src/main.rs");
    let mut dynamic = 0usize;
    let mut rest = source;
    while let Some(index) = rest.find("redis::cmd(") {
        rest = &rest[index + "redis::cmd(".len()..];
        match rest.strip_prefix('"') {
            Some(tail) => {
                let name = tail
                    .split('"')
                    .next()
                    .expect("a quoted command name has a closing quote");
                let name = name.to_ascii_uppercase();
                assert!(
                    READ_ONLY_COMMANDS.contains(&name.as_str()),
                    "src/main.rs sends '{name}', which is not on the read-only allow-list"
                );
                assert!(
                    !FORBIDDEN_COMMANDS.contains(&name.as_str()),
                    "src/main.rs sends '{name}', which mutates the server"
                );
            }
            None => {
                // The only non-literal call allowed is the ladder itself, whose
                // every entry the test above validates against the allow-list.
                assert!(
                    rest.starts_with("command[0])"),
                    "src/main.rs builds a Redis command from something other than a literal \
                     or the probe ladder"
                );
                dynamic += 1;
            }
        }
    }
    assert!(
        dynamic == 1,
        "the probe ladder should be the only computed command (found {dynamic})"
    );
    for forbidden in [
        "redis::cmd(\"SET\"",
        "redis::cmd(\"PUBLISH\"",
        "redis::cmd(\"DEL\"",
        "redis::cmd(\"EXPIRE\"",
        "redis::cmd(\"INCR\"",
    ] {
        assert!(
            !source.contains(forbidden),
            "src/main.rs contains {forbidden}"
        );
    }
}

#[test]
fn nats_probe_connects_without_subscribing_publishing_or_creating_streams() {
    // `test_connection` lives here and must stay a connect-and-drop. Core NATS
    // has no server-side object to create, but JetStream (streams, consumers,
    // KV) and the request/reply inbox do, so none of those may appear.
    let source = include_str!("../../src/service/nats/connect.rs");
    for forbidden in [
        ".publish(",
        ".subscribe(",
        ".queue_subscribe(",
        ".request(",
        ".request_many(",
        "jetstream",
        "JetStream",
        ".add_stream(",
        ".add_consumer(",
        ".create_key_value(",
    ] {
        assert!(
            !source.contains(forbidden),
            "src/service/nats/connect.rs contains `{forbidden}`; the probe must not use it"
        );
    }

    // The probe entry point is exactly a connect plus a drop.
    let probe = source
        .split("pub(crate) async fn test_connection")
        .nth(1)
        .expect("the NATS probe entry point should exist")
        .split("\n}\n")
        .next()
        .unwrap();
    assert!(probe.contains("connect_with_metrics("));
    assert!(
        probe.contains("drop(client)"),
        "the probe should drop the client and do nothing else"
    );
}

#[test]
fn installer_probes_only_through_test_connection_and_check_config() {
    let iss = include_str!("../../installer/redis-conflated-pubsub.iss");

    // Both flags are the only modes the wizard may run the relay in.
    for expected in ["--test-connection", "--check-config"] {
        assert!(iss.contains(expected), "the wizard should use {expected}");
    }
    for forbidden in [
        "--publish ",
        "redis-cli",
        "nats-cli",
        "nats pub",
        "nats stream",
        "nats kv",
        "jetstream",
        "JetStream",
    ] {
        assert!(
            !iss.contains(forbidden),
            "the installer contains `{forbidden}`; it must not write to a backend"
        );
    }
    // Any run of the relay's own executable must be one of the two modes above:
    // an `Exec` of the relay without a flag could run the service against the
    // user's server.
    for line in iss.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("if not Exec(") && !trimmed.starts_with("Exec(") {
            continue;
        }
        if !line.contains("AppExeName") {
            continue;
        }
        assert!(
            line.contains("--test-connection") || line.contains("--check-config"),
            "the installer runs the relay without a validation flag: {trimmed}"
        );
    }

    // The helpers may only validate or install; none of them speaks the wire.
    for (name, source) in [
        (
            "validate-config.ps1",
            include_str!("../../installer/validate-config.ps1"),
        ),
        (
            "verify-service.ps1",
            include_str!("../../installer/verify-service.ps1"),
        ),
        (
            "New-Config.ps1",
            include_str!("../../installer/New-Config.ps1"),
        ),
        ("service.ps1", include_str!("../../installer/service.ps1")),
        (
            "Set-ServiceSecrets.ps1",
            include_str!("../../installer/Set-ServiceSecrets.ps1"),
        ),
    ] {
        for forbidden in [
            "redis-cli",
            "Invoke-RestMethod",
            "TcpClient",
            "PUBLISH",
            "JetStream",
        ] {
            assert!(
                !source.contains(forbidden),
                "installer/{name} contains `{forbidden}`; the installer must not publish or write"
            );
        }
    }
}

#[test]
fn a_denied_command_is_not_a_failed_probe() {
    // The case that started this: a Redis with `stop-writes-on-bgsave-error yes`
    // answers with -MISCONF while serving reads. The server did answer and the
    // login was accepted, so the probe must not call it dead.
    let misconf = redis::make_extension_error(
        "MISCONF".to_owned(),
        Some("Redis is configured to save RDB snapshots, but it is currently unable to persist to disk.".to_owned()),
    );
    assert!(
        !is_auth_failure(&misconf),
        "-MISCONF is not a login failure"
    );
    assert!(
        is_server_reply(&misconf),
        "-MISCONF must count as connected"
    );

    // A read-only ACL user: the relay's credentials work, the command scope does
    // not. Per-command authorization is the user's business, not the probe's.
    let noperm = redis::make_extension_error(
        "NOPERM".to_owned(),
        Some("this user has no permissions to run the 'dbsize' command".to_owned()),
    );
    assert!(!is_auth_failure(&noperm));
    assert!(is_server_reply(&noperm));

    // A replica refusing writes, in case a future probe is pointed at one.
    let readonly = redis::make_extension_error(
        "READONLY".to_owned(),
        Some("You can't write against a read only replica.".to_owned()),
    );
    assert!(!is_auth_failure(&readonly));
    assert!(is_server_reply(&readonly));

    // A rejected login is the one thing that fails the probe.
    let wrong_password = redis::RedisError::from((
        redis::ErrorKind::AuthenticationFailed,
        "Password authentication failed",
    ));
    assert!(is_auth_failure(&wrong_password));

    let noauth = redis::make_extension_error(
        "NOAUTH".to_owned(),
        Some("Authentication required.".to_owned()),
    );
    assert!(is_auth_failure(&noauth));

    // The SELECT failure a password-protected server produces when no password
    // was supplied: the kind is a generic response error, so the detail has to
    // be what classifies it (otherwise a missing password reads as "unreachable").
    let select_unauthenticated = redis::make_extension_error(
        "ERR".to_owned(),
        Some("Redis server refused to switch database - Server(ResponseError): Authentication required.".to_owned()),
    );
    assert!(is_auth_failure(&select_unauthenticated));

    // Any server reply proves the connection and the login; a generic `-ERR`
    // for one command (an old server without it, a proxy) is still an answer.
    let generic = redis::make_extension_error("ERR".to_owned(), Some("unknown command".to_owned()));
    assert!(!is_auth_failure(&generic));
    assert!(
        is_server_reply(&generic),
        "a server error reply proves a live, authenticated connection"
    );

    // A connection-level failure (no reply) is the only other outcome, and it
    // must NOT be mistaken for an answered server.
    let io = redis::RedisError::from((redis::ErrorKind::Io, "connection reset by peer"));
    assert!(!is_auth_failure(&io));
    assert!(!is_server_reply(&io));

    let parse = redis::RedisError::from((
        redis::ErrorKind::Parse,
        "the server returned an unexpected reply",
    ));
    assert!(!is_server_reply(&parse));
}
