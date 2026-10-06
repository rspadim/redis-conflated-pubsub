import json
import os
import select
import time
import urllib.request

from redis_resp import close, connect, read_response, send_command


STATUS_URL = os.environ.get("STATUS_URL", "http://service-oversize:9090/")
OUTPUT_NAME = "oversize-output"
OUTPUT_PATTERN = "oversize-out:*"
PUBLICATIONS = (
    ("oversize-feed:a-large", b"a" * (600 * 1024)),
    ("oversize-feed:b-large", b"b" * (600 * 1024)),
    ("oversize-feed:c-small", b"c" * (200 * 1024)),
)
EXPECTED_SMALL_CHANNEL = b"oversize-out:oversize-feed:c-small"


def wait_for_input_subscription():
    deadline = time.monotonic() + 10
    observed = None
    while time.monotonic() < deadline:
        sock, reader = connect(0)
        send_command(sock, "PUBSUB", "NUMPAT")
        observed = read_response(reader)
        close(sock, reader)
        if observed == 1:
            return
        time.sleep(0.05)
    raise AssertionError(f"Input subscription did not become active; NUMPAT={observed}")


def get_status():
    request = urllib.request.Request(STATUS_URL, method="GET")
    with urllib.request.urlopen(request, timeout=3) as response:
        assert response.status == 200
        return json.loads(response.read())


def wait_for_status(predicate, description, timeout=15):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = get_status()
        if predicate(latest):
            return latest
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}; last status: {latest}")


def main():
    wait_for_input_subscription()

    output_sock, output_reader = connect(1)
    send_command(output_sock, "PSUBSCRIBE", OUTPUT_PATTERN)
    assert read_response(output_reader)[0] == b"psubscribe"
    time.sleep(0.35)

    publisher_sock, publisher_reader = connect(0)
    subscriber_counts = []
    for channel, payload in PUBLICATIONS:
        send_command(publisher_sock, "PUBLISH", channel, payload)
        subscriber_counts.append(read_response(publisher_reader))
    assert all(count >= 1 for count in subscriber_counts), subscriber_counts

    output = wait_for_status(
        lambda status: status.get("state") == "running"
        and status.get("input_messages_total") == len(PUBLICATIONS)
        and status.get("outputs", {}).get(OUTPUT_NAME, {}).get(
            "input_messages_total"
        )
        == len(PUBLICATIONS)
        and status["outputs"][OUTPUT_NAME].get("publish_errors_total") == 1
        and status["outputs"][OUTPUT_NAME].get("publish_error_messages_total") == 2
        and status["outputs"][OUTPUT_NAME].get("uncertain_transactions_total") == 1
        and status["outputs"][OUTPUT_NAME].get("uncertain_messages_total") == 2
        and status["outputs"][OUTPUT_NAME].get("output_messages_total") == 1
        and status["outputs"][OUTPUT_NAME].get("pending_messages") == 0
        and status["outputs"][OUTPUT_NAME].get("pending_payload_bytes") == 0
        and status["outputs"][OUTPUT_NAME].get("pending_keys") == 0,
        "the oversized chunk to fail and the following smaller chunk to publish",
    )

    received = []
    deadline = time.monotonic() + 3
    quiet_deadline = None
    while time.monotonic() < deadline:
        readable, _, _ = select.select([output_sock], [], [], 0.1)
        if not readable:
            if quiet_deadline is not None and time.monotonic() >= quiet_deadline:
                break
            continue
        message = read_response(output_reader)
        assert isinstance(message, list) and message[0] == b"pmessage", message
        received.append((message[2], message[3]))
        quiet_deadline = time.monotonic() + 0.5

    assert received == [(EXPECTED_SMALL_CHANNEL, PUBLICATIONS[2][1])], received
    metrics = output["outputs"][OUTPUT_NAME]
    assert metrics["state"] == "running", metrics
    assert metrics["output_batches_total"] == 1, metrics
    assert metrics["output_payload_bytes_total"] == len(PUBLICATIONS[2][1]), metrics
    assert metrics["uncertain_transactions_total"] == 1, metrics
    assert metrics["uncertain_messages_total"] == 2, metrics
    assert metrics["publish_error_messages_total"] == 2, metrics
    assert metrics["dropped_messages_total"] == 0, metrics
    assert metrics["dropped_payload_bytes_total"] == 0, metrics
    assert metrics["truncated_messages_total"] == 0, metrics
    assert metrics["pending_keys"] == 0, metrics
    assert metrics["pending_payload_bytes"] == 0, metrics
    assert output["input_messages_total"] == len(PUBLICATIONS), output

    close(publisher_sock, publisher_reader)
    close(output_sock, output_reader)
    print(
        "Oversize Redis test passed: a 1 MiB client-query-buffer limit rejected "
        "the first >1 MiB MULTI/EXEC before publishing; the next smaller chunk "
        "was sent once, with no retry or output pause."
    )


if __name__ == "__main__":
    main()
