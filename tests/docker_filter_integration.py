import json
import os
import select
import time
import urllib.request

from redis_resp import close, connect, read_response, send_command


STATUS_URL = os.environ.get("STATUS_URL", "http://service-filter:9090/filters")
OUTPUTS = {
    "regex-filtered": (1, b"out-a:"),
    "glob-filtered": (2, b"out-b:"),
}
INPUT_PREFIX = b"mapped:"
INPUT_SUFFIX = b":source"
OUTPUT_SUFFIX = b":dest"


def wait_for_input_subscription():
    deadline = time.monotonic() + 10
    last_count = None
    while time.monotonic() < deadline:
        sock, reader = connect(0)
        send_command(sock, "PUBSUB", "NUMPAT")
        last_count = read_response(reader)
        close(sock, reader)
        if last_count == 1:
            return
        time.sleep(0.05)
    raise AssertionError(f"Input subscription did not become active; NUMPAT={last_count}")


def open_subscribers():
    subscribers = {}
    for name, (database, prefix) in OUTPUTS.items():
        sock, reader = connect(database)
        send_command(sock, "PSUBSCRIBE", prefix + b"*")
        acknowledgement = read_response(reader)
        assert acknowledgement[0] == b"psubscribe", acknowledgement
        subscribers[name] = (sock, reader)
    return subscribers


def publish(channel, payload):
    sock, reader = connect(0)
    try:
        send_command(sock, "PUBLISH", channel, payload)
        return read_response(reader)
    finally:
        close(sock, reader)


def receive_expected(subscriber, expected, timeout=5):
    sock, reader = subscriber
    readable, _, _ = select.select([sock], [], [], timeout)
    assert readable, f"Timed out waiting for {expected!r}"
    response = read_response(reader)
    assert isinstance(response, list) and response[0] == b"pmessage", response
    actual = response[2], response[3]
    assert actual == expected, {"expected": expected, "actual": actual}


def assert_quiet(subscribers, duration=0.2):
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        readable, _, _ = select.select(
            [sock for sock, _ in subscribers.values()], [], [], 0.02
        )
        assert not readable, "a denied filter unexpectedly published a message"


def mapped_output(prefix, source_channel):
    return prefix + INPUT_PREFIX + source_channel + INPUT_SUFFIX + OUTPUT_SUFFIX


def assert_filter_caches():
    with urllib.request.urlopen(STATUS_URL, timeout=5) as response:
        assert response.status == 200
        snapshot = json.loads(response.read())
    caches = snapshot["caches"]

    def entries(name):
        return {
            entry["channel"]: entry["value"]
            for entry in caches[name]["entries_most_recent_first"]
        }

    assert entries("input.filters")["feed:input-denied"] == "deny"
    assert caches["input.filters"]["capacity"] == 8
    assert (
        entries("outputs.regex-filtered.filters")[
            "mapped:feed:regex-denied:source"
        ]
        == "deny"
    )
    assert (
        entries("outputs.glob-filtered.filters")["mapped:feed:glob-denied:source"]
        == "deny"
    )
    assert caches["outputs.regex-filtered.filters"]["capacity"] == 12
    assert caches["outputs.regex-filtered.channel_policies"]["capacity"] == 5
    assert caches["outputs.glob-filtered.filters"]["capacity"] == 10
    assert caches["outputs.glob-filtered.channel_policies"]["capacity"] == 7
    assert "outputs.regex-filtered.channel_policies" in caches
    assert "outputs.glob-filtered.channel_policies" in caches


def main():
    wait_for_input_subscription()
    subscribers = open_subscribers()
    payload = b"\x00filter-test:\xff"
    try:
        assert publish(b"feed:input-denied", payload) >= 0
        assert_quiet(subscribers)

        assert publish(b"feed:regex-denied", payload) >= 0
        receive_expected(
            subscribers["glob-filtered"],
            (mapped_output(b"out-b:", b"feed:regex-denied"), payload),
        )
        assert_quiet({"regex-filtered": subscribers["regex-filtered"]})

        assert publish(b"feed:glob-denied", payload) >= 0
        receive_expected(
            subscribers["regex-filtered"],
            (mapped_output(b"out-a:", b"feed:glob-denied"), payload),
        )
        assert_quiet({"glob-filtered": subscribers["glob-filtered"]})

        # This deny pattern includes the output namespace. It must not match because
        # output filters run before output.channel_prefix/channel_suffix are applied.
        assert publish(b"feed:output-prefix-leak", payload) >= 0
        for name, (_, prefix) in OUTPUTS.items():
            receive_expected(
                subscribers[name],
                (mapped_output(prefix, b"feed:output-prefix-leak"), payload),
            )

        assert publish(b"feed:allowed", payload) >= 0
        for name, (_, prefix) in OUTPUTS.items():
            receive_expected(
                subscribers[name],
                (mapped_output(prefix, b"feed:allowed"), payload),
            )
        assert_quiet(subscribers)
        assert_filter_caches()
    finally:
        for sock, reader in subscribers.values():
            close(sock, reader)

    print(
        "Filter E2E passed: input glob deny ran on the source channel; output regex/glob "
        "denies ran after subscription mapping but before output namespaces; defaults accepted."
    )


if __name__ == "__main__":
    main()
