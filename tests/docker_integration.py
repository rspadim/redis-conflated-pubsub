import base64
import json
import select
import time
import urllib.request

from redis_resp import close, connect, read_response, send_command


OUTPUT_PREFIX = b"replica:"
DIRECT_CHANNEL = "test-direct:marker"
OUTPUT_MARKER_CHANNEL = "replica:test-direct:marker"
OUTPUT_FEED_PATTERN = "replica:test-feed:*"
OUTPUT_LOOP_PATTERN = "replica:replica:*"
READY_CHANNEL = "test-control:ready"
DONE_CHANNEL = "test-control:done"
FEED_CHANNELS = {
    "test-feed:alpha",
    "test-feed:beta",
    "test-feed:gamma",
}
EXPECTED_PATTERN_COUNT = 4
STATUS_URL = "http://service:9090/"


def wait_for_input_subscriptions():
    deadline = time.monotonic() + 10
    observed = None
    while time.monotonic() < deadline:
        sock, reader = connect(0)
        send_command(sock, "PUBSUB", "NUMPAT")
        pattern_count = read_response(reader)
        send_command(sock, "PUBSUB", "NUMSUB", DIRECT_CHANNEL)
        direct_subscribers = read_response(reader)
        close(sock, reader)
        observed = (pattern_count, direct_subscribers)
        if (
            pattern_count == EXPECTED_PATTERN_COUNT
            and direct_subscribers[0] == DIRECT_CHANNEL.encode()
            and direct_subscribers[1] >= 1
        ):
            return
        time.sleep(0.05)
    raise AssertionError(
        f"The service did not establish its input subscriptions; last counts: {observed}"
    )


def wait_for_random_publisher():
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        sock, reader = connect(2)
        send_command(sock, "PUBSUB", "NUMSUB", READY_CHANNEL)
        result = read_response(reader)
        close(sock, reader)
        if result and result[0] == READY_CHANNEL.encode() and result[1] >= 1:
            return
        time.sleep(0.05)
    raise AssertionError("The random publisher did not subscribe to its ready channel")


def read_output_message(sock, reader, timeout=5):
    sock.settimeout(timeout)
    while True:
        message = read_response(reader)
        if isinstance(message, list) and message[0] == b"pmessage" and len(message) == 4:
            channel, payload = message[2], message[3]
        elif isinstance(message, list) and message[0] == b"message" and len(message) == 3:
            channel, payload = message[1], message[2]
        else:
            continue
        if channel.startswith(OUTPUT_PREFIX):
            return channel.decode("utf-8"), payload


def publish(sock, reader, channel, payload):
    send_command(sock, "PUBLISH", channel, payload)
    return read_response(reader)


def get_status():
    request = urllib.request.Request(STATUS_URL, method="GET")
    with urllib.request.urlopen(request, timeout=3) as response:
        assert response.status == 200
        return json.loads(response.read())


def wait_for_status(predicate, description, timeout=5):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = get_status()
        if predicate(latest):
            return latest
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}; last status: {latest}")


def assert_database_selection():
    sock, reader = connect(2)
    send_command(sock, "CLIENT", "LIST")
    client_list = read_response(reader)
    close(sock, reader)

    clients = []
    for line in client_list.decode("utf-8").splitlines():
        clients.append(
            dict(field.split("=", 1) for field in line.split() if "=" in field)
        )

    assert any(client.get("db") == "0" and "P" in client.get("flags", "") for client in clients), (
        "the service input Pub/Sub connection is not using DB0"
    )
    assert any(client.get("db") == "1" and "P" not in client.get("flags", "") for client in clients), (
        "the service output connection is not using DB1"
    )
    assert any(client.get("db") == "2" and "P" in client.get("flags", "") for client in clients), (
        "the randomized publisher is not using DB2"
    )


def decode_expected_payloads(completion):
    encoded = completion["latest_payloads"]
    assert set(encoded) == FEED_CHANNELS, encoded
    decoded = {}
    for channel, item in encoded.items():
        if item["encoding"] == "base64":
            decoded[channel] = base64.b64decode(item["data"], validate=True)
        elif item["encoding"] == "utf-8":
            decoded[channel] = item["data"].encode("utf-8")
        else:
            raise AssertionError(f"Unsupported test-control encoding: {item}")
    return decoded


def collect_final_messages(sock, reader, expected):
    values = {}
    output_channels = set()
    message_count = 0
    deadline = time.monotonic() + 8
    quiet_deadline = None

    while time.monotonic() < deadline:
        readable, _, _ = select.select([sock], [], [], 0.05)
        if not readable:
            if quiet_deadline is not None and time.monotonic() >= quiet_deadline:
                break
            continue

        output_channel, payload = read_output_message(sock, reader)
        message_count += 1
        assert output_channel.startswith("replica:test-feed:"), output_channel
        source_channel = output_channel[len("replica:") :]
        assert source_channel in expected, output_channel
        output_channels.add(output_channel)
        values[source_channel] = payload

        if set(values) == set(expected):
            quiet_deadline = time.monotonic() + 0.6
        else:
            quiet_deadline = None

    expected_output_channels = {
        f"replica:{channel}" for channel in expected
    }
    assert output_channels == expected_output_channels, {
        "actual": output_channels,
        "expected": expected_output_channels,
    }
    assert message_count == len(expected), {
        "actual_message_count": message_count,
        "expected_message_count": len(expected),
    }
    assert values == expected, {"actual": values, "expected": expected}
    return message_count


def main():
    output_sock, output_reader = connect(1)
    send_command(output_sock, "SUBSCRIBE", OUTPUT_MARKER_CHANNEL)
    assert read_response(output_reader)[0] == b"subscribe"
    send_command(output_sock, "PSUBSCRIBE", OUTPUT_FEED_PATTERN)
    assert read_response(output_reader)[0] == b"psubscribe"
    send_command(output_sock, "PSUBSCRIBE", OUTPUT_LOOP_PATTERN)
    assert read_response(output_reader)[0] == b"psubscribe"

    control_sock, control_reader = connect(2)
    send_command(control_sock, "SUBSCRIBE", DONE_CHANNEL)
    assert read_response(control_reader)[0] == b"subscribe"

    wait_for_input_subscriptions()
    wait_for_random_publisher()

    publisher_sock, publisher_reader = connect(2)
    assert publish(publisher_sock, publisher_reader, DIRECT_CHANNEL, b"ready") == 1
    marker_channel, marker_payload = read_output_message(output_sock, output_reader)
    assert marker_channel == "replica:test-direct:marker"
    assert marker_payload == b"ready"

    baseline = wait_for_status(
        lambda status: status["output_messages_total"] >= 1
        and status["excluded_messages_total"] >= 1,
        "the direct-channel output and its excluded feedback",
    )
    assert baseline["state"] == "running", baseline
    assert baseline["output_batches_total"] == 1, baseline
    assert baseline["output_messages_total"] == 1, baseline
    assert baseline["input_messages_total"] == 1, baseline
    assert baseline["conflated_messages_total"] == 0, baseline
    assert baseline["excluded_messages_total"] == 1, baseline
    assert baseline["dropped_messages_total"] == 0, baseline
    assert_database_selection()

    assert publish(publisher_sock, publisher_reader, READY_CHANNEL, b"start") >= 1
    done_message = read_response(control_reader)
    assert done_message[0] == b"message" and done_message[1] == DONE_CHANNEL.encode()
    completion = json.loads(done_message[2])
    expected = decode_expected_payloads(completion)
    published_feed_count = completion["published_feed_count"]
    assert published_feed_count > len(expected)

    assert expected["test-feed:alpha"].startswith(b"\x00\xff")
    assert expected["test-feed:beta"].decode("ascii").startswith("final:")
    assert expected["test-feed:gamma"].decode("ascii").startswith("final:")

    forwarded_message_count = collect_final_messages(
        output_sock, output_reader, expected
    )

    readable, _, _ = select.select([output_sock], [], [], 1.0)
    assert not readable, "the service republished its own output"

    status = wait_for_status(
        lambda current: current["excluded_messages_total"]
        >= baseline["excluded_messages_total"] + len(expected),
        "the output-prefix feedback to be filtered",
    )
    assert status["state"] == "running", status
    assert status["input_messages_total"] == baseline["input_messages_total"] + published_feed_count, status
    assert status["output_batches_total"] == baseline["output_batches_total"] + 1, status
    assert status["output_messages_total"] == baseline["output_messages_total"] + len(expected), status
    assert status["conflated_messages_total"] == (
        baseline["conflated_messages_total"] + published_feed_count - len(expected)
    ), status
    assert status["excluded_messages_total"] == (
        baseline["excluded_messages_total"] + len(expected)
    ), status
    assert status["dropped_messages_total"] == baseline["dropped_messages_total"], status
    assert status["publish_errors_total"] == baseline["publish_errors_total"], status
    assert forwarded_message_count == len(expected), status

    close(publisher_sock, publisher_reader)
    close(control_sock, control_reader)
    close(output_sock, output_reader)
    print(
        "Docker Redis Pub/Sub test passed: DB2 publisher -> DB0 SUBSCRIBE/PSUBSCRIBE "
        f"-> DB1 prefixed output; {published_feed_count} feed messages conflated into "
        f"{forwarded_message_count} raw messages in one MULTI/EXEC batch."
    )


if __name__ == "__main__":
    main()
