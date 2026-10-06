import json
import os
import select
import socket
import time
import urllib.request
from collections import Counter

from redis_resp import close, connect, read_response, send_command


STATUS_URL = os.environ.get("STATUS_URL", "http://10.253.242.20:9090/")
PROXY_HOST = os.environ.get("PROXY_HOST", "10.253.242.50")
PROXY_STATS_PORT = int(os.environ.get("PROXY_STATS_PORT", "9091"))
OUTPUT_NAMES = ("chunked", "send", "truncate", "drop", "immediate")
REDIS_QUERY_LIMIT = 1024 * 1024
CHUNK_TARGET = 700 * 1024
SMALL_TARGET = 220 * 1024
SMALL_PAYLOAD_BYTES = 100 * 1024
LARGE_PAYLOAD_BYTES = 300 * 1024


def make_payload(tag, length):
    prefix = b"\x00\xff" + tag.encode("ascii") + b":"
    return prefix + bytes((tag[-1].encode("ascii")[0],)) * (length - len(prefix))


PUBLICATIONS = (
    ("oversize-feed:a", make_payload("a-old", SMALL_PAYLOAD_BYTES)),
    ("oversize-feed:a", make_payload("a-latest", SMALL_PAYLOAD_BYTES)),
    ("oversize-feed:b", make_payload("b-large", LARGE_PAYLOAD_BYTES)),
    ("oversize-feed:c", make_payload("c-large", LARGE_PAYLOAD_BYTES)),
    ("oversize-feed:d", make_payload("d-large", LARGE_PAYLOAD_BYTES)),
    ("oversize-feed:e", make_payload("e-large", LARGE_PAYLOAD_BYTES)),
    ("oversize-feed:z", make_payload("z-large", LARGE_PAYLOAD_BYTES)),
)
LATEST = dict(PUBLICATIONS)
SENTINEL_PUBLICATIONS = (
    ("__sentinel__:hello", b"master,127.0.0.1,6379,runid"),
    ("+switch-master", b"master 127.0.0.1 6379 127.0.0.2 6380"),
)


def output_channel(output_name, source_channel):
    return f"out:{output_name}:{source_channel}".encode("ascii")


def bulk_resp_bytes(length):
    return length + len(str(length)) + 5


def publish_resp_bytes(channel_length, payload_length):
    return 4 + bulk_resp_bytes(7) + bulk_resp_bytes(channel_length) + bulk_resp_bytes(
        payload_length
    )


def transaction_bytes(channel_length, payload_length):
    return 15 + publish_resp_bytes(channel_length, payload_length) + 14


def max_payload_for_transaction(channel, target, maximum):
    low = 0
    high = maximum
    best = -1
    while low <= high:
        candidate = (low + high) // 2
        if transaction_bytes(len(channel), candidate) <= target:
            best = candidate
            low = candidate + 1
        else:
            high = candidate - 1
    if best < 0:
        raise AssertionError("The channel and RESP framing exceed the test target")
    return best


def wait_for_input_subscription():
    deadline = time.monotonic() + 15
    last_count = None
    while time.monotonic() < deadline:
        sock, reader = connect(0)
        send_command(sock, "PUBSUB", "NUMPAT")
        last_count = read_response(reader)
        close(sock, reader)
        if last_count == 3:
            return
        time.sleep(0.05)
    raise AssertionError(f"Input subscription did not become active; NUMPAT={last_count}")


def get_status():
    request = urllib.request.Request(STATUS_URL, method="GET")
    with urllib.request.urlopen(request, timeout=3) as response:
        assert response.status == 200
        return json.loads(response.read())


def output_metrics(status):
    outputs = status.get("outputs")
    assert isinstance(outputs, dict) and set(outputs) == set(OUTPUT_NAMES), outputs
    return outputs


def wait_for_status(predicate, description, timeout=20):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = get_status()
        if predicate(latest):
            return latest
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}; last status: {latest}")


def open_subscribers():
    subscribers = {}
    for index, name in enumerate(OUTPUT_NAMES, start=1):
        sock, reader = connect(index)
        send_command(sock, "PSUBSCRIBE", f"out:{name}:*")
        acknowledgement = read_response(reader)
        assert acknowledgement[0] == b"psubscribe", acknowledgement
        subscribers[name] = (sock, reader)
    return subscribers


def receive_message(sock, reader):
    message = read_response(reader)
    if isinstance(message, list) and message[0] == b"pmessage" and len(message) == 4:
        return message[2], message[3]
    raise AssertionError(f"Unexpected Redis Pub/Sub response: {message!r}")


def collect_exact(subscriber, expected, timeout=10):
    sock, reader = subscriber
    expected = Counter(expected)
    received = []
    deadline = time.monotonic() + timeout
    while Counter(received) != expected and time.monotonic() < deadline:
        readable, _, _ = select.select([sock], [], [], 0.1)
        if readable:
            received.append(receive_message(sock, reader))
            if len(received) > sum(expected.values()):
                raise AssertionError(f"Received duplicate or unexpected output: {received!r}")
    assert Counter(received) == expected, {
        "received_count": len(received),
        "expected_count": sum(expected.values()),
    }
    readable, _, _ = select.select([sock], [], [], 0.5)
    assert not readable, "An output emitted duplicate Pub/Sub events"
    return received


def get_proxy_stats():
    with socket.create_connection((PROXY_HOST, PROXY_STATS_PORT), timeout=3) as sock:
        sock.settimeout(3)
        sock.sendall(b"STATS\r\n")
        response = bytearray()
        while not response.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                raise RuntimeError("counting proxy closed its stats connection")
            response.extend(chunk)
    return json.loads(response)


def publish_feed():
    sock, reader = connect(0)
    try:
        for channel, payload in PUBLICATIONS:
            send_command(sock, "PUBLISH", channel, payload)
            subscriber_count = read_response(reader)
            assert subscriber_count >= 1, (channel, subscriber_count)
    finally:
        close(sock, reader)


def publish_sentinel_messages():
    sock, reader = connect(0)
    try:
        for channel, payload in SENTINEL_PUBLICATIONS:
            send_command(sock, "PUBLISH", channel, payload)
            subscriber_count = read_response(reader)
            assert subscriber_count >= 1, (channel, subscriber_count)
    finally:
        close(sock, reader)


def main():
    wait_for_input_subscription()
    subscribers = open_subscribers()

    # Let every positive interval reach its first tick before sending the burst.
    time.sleep(1.7)
    publish_feed()
    publish_sentinel_messages()

    expected_truncate = {}
    for source, payload in LATEST.items():
        channel = output_channel("truncate", source)
        if len(payload) > SMALL_TARGET:
            length = max_payload_for_transaction(channel, SMALL_TARGET, len(payload))
            expected_truncate[source] = (payload[:length], len(payload) - length)
        else:
            expected_truncate[source] = (payload, 0)
    truncated_bytes = sum(removed for _, removed in expected_truncate.values())
    expected_counts = {
        "chunked": len(LATEST),
        "send": len(LATEST),
        "truncate": len(LATEST),
        "drop": 1,
        "immediate": len(PUBLICATIONS),
    }

    status = wait_for_status(
        lambda current: current.get("state") == "running"
        and current.get("input_messages_total") == len(PUBLICATIONS)
        and current.get("excluded_messages_total") == len(SENTINEL_PUBLICATIONS)
        and all(
            output_metrics(current)[name].get("output_messages_total") == count
            and output_metrics(current)[name].get("pending_messages") == 0
            and output_metrics(current)[name].get("publish_errors_total") == 0
            for name, count in expected_counts.items()
        ),
        "all policies and the immediate output to finish",
    )

    expected_latest_messages = {
        name: [
            (output_channel(name, source), payload)
            for source, payload in LATEST.items()
        ]
        for name in ("chunked", "send")
    }
    expected_latest_messages["truncate"] = [
        (output_channel("truncate", source), payload)
        for source, (payload, _) in expected_truncate.items()
    ]
    expected_latest_messages["drop"] = [
        (output_channel("drop", "oversize-feed:a"), LATEST["oversize-feed:a"])
    ]
    expected_latest_messages["immediate"] = [
        (output_channel("immediate", source), payload)
        for source, payload in PUBLICATIONS
    ]
    received = {
        name: collect_exact(subscribers[name], expected)
        for name, expected in expected_latest_messages.items()
    }

    outputs = output_metrics(status)
    expected_batches = {"chunked": 3, "send": 6, "truncate": 6, "drop": 1, "immediate": 7}
    expected_conflated = {"chunked": 1, "send": 1, "truncate": 1, "drop": 1, "immediate": 0}
    for name in OUTPUT_NAMES:
        metrics = outputs[name]
        assert metrics["input_messages_total"] == len(PUBLICATIONS), metrics
        assert metrics["output_batches_total"] == expected_batches[name], metrics
        assert metrics["conflated_messages_total"] == expected_conflated[name], metrics
        assert metrics["publish_errors_total"] == 0, metrics
        assert metrics["pending_payload_bytes"] == 0, metrics
    assert status["output_batches_total"] == sum(expected_batches.values()), status
    assert status["output_messages_total"] == sum(expected_counts.values()), status
    assert status["conflated_messages_total"] == sum(expected_conflated.values()), status
    assert status["publish_errors_total"] == 0, status
    assert outputs["truncate"]["truncated_messages_total"] == 5, outputs["truncate"]
    assert outputs["truncate"]["truncated_payload_bytes_total"] == truncated_bytes, (
        outputs["truncate"]
    )
    assert outputs["drop"]["dropped_messages_total"] == 5, outputs["drop"]
    assert outputs["immediate"]["output_messages_total"] == len(PUBLICATIONS), (
        outputs["immediate"]
    )
    assert status["dropped_messages_total"] == 5, status
    assert status["truncated_messages_total"] == 5, status
    assert status["truncated_payload_bytes_total"] == truncated_bytes, status
    assert status["uncertain_transactions_total"] == 0, status
    assert status["excluded_messages_total"] == len(SENTINEL_PUBLICATIONS), status

    expected_target_for_output = {
        "chunked": CHUNK_TARGET,
        "send": SMALL_TARGET,
        "truncate": SMALL_TARGET,
        "drop": SMALL_TARGET,
        "immediate": SMALL_TARGET,
    }
    proxy = get_proxy_stats()
    assert proxy["commands"]["MULTI"] == sum(expected_batches[name] for name in OUTPUT_NAMES if name != "immediate"), proxy
    assert proxy["commands"]["EXEC"] == proxy["commands"]["MULTI"], proxy
    assert proxy["commands"]["PUBLISH"] == sum(expected_counts.values()), proxy
    for name in OUTPUT_NAMES:
        metrics = proxy["outputs"][name]
        assert metrics["publish_count"] == expected_counts[name], (name, metrics)
        assert metrics["transaction_count"] == (
            0 if name == "immediate" else expected_batches[name]
        ), (name, metrics)
        assert metrics["direct_publish_count"] == (
            len(PUBLICATIONS) if name == "immediate" else 0
        ), (name, metrics)
        limit = expected_target_for_output[name]
        for request_bytes in metrics["transaction_request_bytes"]:
            assert request_bytes <= REDIS_QUERY_LIMIT, (name, request_bytes)
            if name in ("chunked", "truncate", "drop"):
                assert request_bytes <= limit, (name, request_bytes, limit)
        for request_bytes in metrics["direct_request_bytes"]:
            assert request_bytes <= REDIS_QUERY_LIMIT, (name, request_bytes)
    assert len(proxy["outputs"]["send"]["transaction_request_bytes"]) == 6
    assert sum(
        request_bytes > SMALL_TARGET
        for request_bytes in proxy["outputs"]["send"]["transaction_request_bytes"]
    ) == 5, proxy["outputs"]["send"]
    assert proxy["outputs"]["immediate"]["transaction_request_bytes"] == [], proxy

    for sock, reader in subscribers.values():
        close(sock, reader)
    print(
        "Protocol E2E passed: Pub/Sub burst was conflated and split below Redis's "
        "1 MiB query limit; send/truncate/drop policies behaved as configured; "
        "interval_ms=0 emitted only direct PUBLISH commands, with no MULTI/EXEC."
    )


if __name__ == "__main__":
    main()
