import json
import os
import select
import socket
import time
import urllib.request
from collections import Counter

from redis_resp import close, connect, read_response, send_command


PROXY_HOST = os.environ.get("PROXY_HOST", "fault-proxy")
PROXY_STATS_PORT = 9091
FAULT_APP_HOST = os.environ.get("FAULT_APP_HOST", "fault-service")
STATUS_URL = f"http://{FAULT_APP_HOST}:9090/"
OUTPUT_NAME = "output1"
OUTPUT_PREFIX = b"fault-output:mapped:"
OUTPUT_SUFFIX = b":source"
OUTPUT_PATTERN = "fault-output:*"
MIN_INPUT_PATTERN_COUNT = 2
COMMITTED_CHUNK = (("fault-feed:alpha", b"committed-alpha:\x00\xff"),)
FAILED_CHUNK = (
    ("fault-feed:beta", b"uncertain-beta:\xfe\x00"),
    ("fault-feed:gamma", b"uncertain-gamma:\x00\xfd"),
)
LATER_MESSAGE = ("fault-feed:delta", b"later-delta:\x00\xfc")


def expected_output_channel(source_channel):
    return OUTPUT_PREFIX + source_channel.encode("utf-8") + OUTPUT_SUFFIX


def wait_for_input_subscriptions():
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        sock, reader = connect(0)
        send_command(sock, "PUBSUB", "NUMPAT")
        pattern_count = read_response(reader)
        close(sock, reader)
        if pattern_count >= MIN_INPUT_PATTERN_COUNT:
            return
        time.sleep(0.05)
    raise AssertionError("The fault-test app did not establish its input patterns")


def publish_many(publications):
    sock, reader = connect(0)
    for channel, payload in publications:
        send_command(sock, "PUBLISH", channel, payload)
    counts = [read_response(reader) for _ in publications]
    close(sock, reader)
    assert all(count >= 1 for count in counts), counts


def read_output(sock, reader):
    message = read_response(reader)
    if isinstance(message, list) and message[0] == b"pmessage" and len(message) == 4:
        return message[2], message[3]
    raise AssertionError(f"Unexpected Redis Pub/Sub response: {message!r}")


def read_chunk(sock, reader, chunk, timeout=8):
    expected = Counter(
        (expected_output_channel(channel), payload) for channel, payload in chunk
    )
    received = []
    expected_sources = {channel for channel, _ in chunk}
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and len(received) < len(chunk):
        readable, _, _ = select.select([sock], [], [], 0.1)
        if not readable:
            continue
        channel, payload = read_output(sock, reader)
        source_channel = next(
            (
                source
                for source in expected_sources
                if expected_output_channel(source) == channel
            ),
            None,
        )
        assert source_channel is not None, channel
        received.append((channel, payload))
    assert Counter(received) == expected, {
        "actual": received,
        "expected": list(expected.elements()),
    }
    return received


def get_status():
    request = urllib.request.Request(STATUS_URL, method="GET")
    with urllib.request.urlopen(request, timeout=3) as response:
        assert response.status == 200
        return json.loads(response.read())


def output_metrics(status):
    outputs = status.get("outputs")
    assert isinstance(outputs, dict) and set(outputs) == {OUTPUT_NAME}, outputs
    return outputs[OUTPUT_NAME]


def wait_for_status(predicate, description, timeout=10):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = get_status()
        if predicate(latest):
            return latest
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}; last status: {latest}")


def get_proxy_stats():
    with socket.create_connection((PROXY_HOST, PROXY_STATS_PORT), timeout=3) as sock:
        sock.settimeout(3)
        sock.sendall(b"STATS\r\n")
        response = bytearray()
        while not response.endswith(b"\n\n"):
            chunk = sock.recv(4096)
            if not chunk:
                raise RuntimeError("fault proxy closed its stats connection")
            response.extend(chunk)
    return {
        key: int(value)
        for line in response.decode("ascii").splitlines()
        if line and "=" in line
        for key, value in [line.split("=", 1)]
    }


def wait_for_proxy_stats(predicate, description, timeout=10):
    deadline = time.monotonic() + timeout
    latest = {}
    while time.monotonic() < deadline:
        latest = get_proxy_stats()
        if predicate(latest):
            return latest
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}; proxy stats: {latest}")


def wait_for_uncertain_result(expected_messages):
    return wait_for_status(
        lambda status: status.get("state") == "running"
        and output_metrics(status).get("state") == "running"
        and output_metrics(status).get("uncertain_transactions_total") == 1
        and output_metrics(status).get("uncertain_messages_total") == expected_messages
        and output_metrics(status).get("pending_messages") == 0,
        "the uncertain transaction to be counted without pausing the output",
    )


def assert_no_output(sock, timeout=1.0):
    readable, _, _ = select.select([sock], [], [], timeout)
    assert not readable, "the app duplicated an already-delivered output event"


def main():
    wait_for_input_subscriptions()
    output_sock, output_reader = connect(1)
    send_command(output_sock, "PSUBSCRIBE", OUTPUT_PATTERN)
    assert read_response(output_reader)[0] == b"psubscribe"

    # Allow the output interval to advance before producing the two test batches.
    time.sleep(0.35)

    publish_many(COMMITTED_CHUNK)
    committed_messages = read_chunk(output_sock, output_reader, COMMITTED_CHUNK)
    committed_status = wait_for_status(
        lambda status: status.get("state") == "running"
        and output_metrics(status).get("state") == "running"
        and output_metrics(status).get("output_batches_total") == 1
        and output_metrics(status).get("output_messages_total") == len(COMMITTED_CHUNK)
        and output_metrics(status).get("pending_messages") == 0,
        "the pre-fault chunk to commit successfully",
    )
    assert committed_status["outputs"][OUTPUT_NAME]["pending_payload_bytes"] == 0
    proxy_stats = wait_for_proxy_stats(
        lambda stats: stats.get("successful_execs") == 1,
        "the first successful MULTI/EXEC",
    )
    assert proxy_stats["multi_exec_attempts"] == 1, proxy_stats
    assert proxy_stats["dropped_exec_replies"] == 0, proxy_stats

    publish_many(FAILED_CHUNK)
    failed_messages = read_chunk(output_sock, output_reader, FAILED_CHUNK)
    uncertain_status = wait_for_uncertain_result(len(FAILED_CHUNK))
    proxy_stats = wait_for_proxy_stats(
        lambda stats: stats.get("dropped_exec_replies") == 1,
        "the proxy to drop the second successful EXEC reply",
    )
    assert proxy_stats["multi_exec_attempts"] == 2, proxy_stats
    assert proxy_stats["successful_execs"] == 2, proxy_stats
    assert proxy_stats["drop_publish_count"] == len(FAILED_CHUNK), proxy_stats
    uncertain_output = uncertain_status["outputs"][OUTPUT_NAME]
    assert uncertain_output["publish_errors_total"] == 1, uncertain_status
    assert uncertain_output["publish_error_messages_total"] == len(FAILED_CHUNK), (
        uncertain_status
    )

    publish_many((LATER_MESSAGE,))
    later_messages = read_chunk(output_sock, output_reader, (LATER_MESSAGE,))
    final_status = wait_for_status(
        lambda status: status.get("state") == "running"
        and output_metrics(status).get("state") == "running"
        and output_metrics(status).get("output_messages_total")
        == len(COMMITTED_CHUNK) + 1
        and output_metrics(status).get("pending_messages") == 0
        and status.get("excluded_messages_total")
        == len(COMMITTED_CHUNK) + len(FAILED_CHUNK) + 1,
        # The echo filter runs on a separate input task; wait for all executed PUBLISHes.
        "a later message to publish after the uncertain transaction",
    )
    assert_no_output(output_sock, 1.0)
    final_output = output_metrics(final_status)
    assert final_output["state"] == "running", final_status
    assert final_output["input_messages_total"] == (
        len(COMMITTED_CHUNK) + len(FAILED_CHUNK) + 1
    ), final_status
    assert final_output["output_batches_total"] == 2, final_status
    assert final_output["output_messages_total"] == (
        len(COMMITTED_CHUNK) + 1
    ), final_status
    assert final_output["pending_messages"] == 0, final_status
    assert final_output["pending_keys"] == 0, final_status
    assert final_output["pending_payload_bytes"] == 0, final_status
    assert final_output["uncertain_transactions_total"] == 1, final_status
    assert final_output["uncertain_messages_total"] == len(FAILED_CHUNK), final_status
    assert final_status["input_messages_total"] == (
        len(COMMITTED_CHUNK) + len(FAILED_CHUNK) + 1
    ), final_status
    assert final_status["excluded_messages_total"] == (
        len(COMMITTED_CHUNK) + len(FAILED_CHUNK) + 1
    ), final_status
    assert final_output["publish_errors_total"] == 1, final_status
    assert final_output["publish_error_messages_total"] == len(FAILED_CHUNK), (
        final_status
    )

    proxy_stats = get_proxy_stats()
    assert proxy_stats["multi_exec_attempts"] == 3, proxy_stats
    assert proxy_stats["successful_execs"] == 3, proxy_stats
    assert proxy_stats["dropped_exec_replies"] == 1, proxy_stats
    assert proxy_stats["drop_publish_count"] == len(FAILED_CHUNK), proxy_stats
    assert proxy_stats["execs_after_drop"] == 1, proxy_stats
    assert proxy_stats["publishes_after_drop"] == 1, proxy_stats
    assert len(committed_messages) == len(COMMITTED_CHUNK), committed_messages
    assert len(failed_messages) == len(FAILED_CHUNK), failed_messages
    assert len(later_messages) == 1, later_messages

    close(output_sock, output_reader)
    print(
        "Docker fault-injection test passed: Redis executed the ambiguous chunk once "
        "before its reply was dropped; the app did not retry it, counted uncertainty, "
        "and successfully published a later chunk without pausing the output."
    )


if __name__ == "__main__":
    main()
