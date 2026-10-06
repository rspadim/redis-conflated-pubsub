import json
import os
import select
import time
import urllib.request
from collections import Counter

from payloads import FEED_CHANNELS, make_publications
from redis_resp import close, connect, read_response, send_command


OUTPUT0_NAME = "output0"
OUTPUT1_NAME = "output1"
FEED_PREFIX = b"mapped:"
FEED_SUFFIX = b":source"
OUTPUT0_PREFIX = b"db0:"
OUTPUT0_SUFFIX = b""
OUTPUT1_PREFIX = b"db1:"
OUTPUT1_SUFFIX = b""
OUTPUT0_PATTERN = "db0:*"
OUTPUT1_PATTERN = "db1:*"
START_CHANNEL = "test-control:start"
DONE_CHANNEL = "test-control:done"
FEED_CHANNELS = set(FEED_CHANNELS)
EXPECTED_INPUT_PATTERN_COUNT = 3
STATUS_URL = os.environ.get("STATUS_URL", "http://service:9090/")


def wait_for_input_subscriptions():
    deadline = time.monotonic() + 10
    observed = None
    while time.monotonic() < deadline:
        sock, reader = connect(0)
        send_command(sock, "PUBSUB", "NUMPAT")
        pattern_count = read_response(reader)
        close(sock, reader)
        observed = pattern_count
        if pattern_count == EXPECTED_INPUT_PATTERN_COUNT:
            return
        time.sleep(0.05)
    raise AssertionError(
        f"The service did not establish all input patterns; last count: {observed}"
    )


def wait_for_random_publisher():
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        sock, reader = connect(0)
        send_command(sock, "PUBSUB", "NUMSUB", START_CHANNEL)
        result = read_response(reader)
        close(sock, reader)
        if result and result[0] == START_CHANNEL.encode() and result[1] >= 1:
            return
        time.sleep(0.05)
    raise AssertionError("The random publisher did not subscribe to its start channel")


def read_output_message(sock, reader):
    message = read_response(reader)
    if isinstance(message, list) and message[0] == b"pmessage" and len(message) == 4:
        return message[2], message[3]
    raise AssertionError(f"Unexpected Redis Pub/Sub response: {message!r}")


def expected_output_channel(output_prefix, output_suffix, source_channel):
    return (
        output_prefix
        + FEED_PREFIX
        + source_channel.encode("utf-8")
        + FEED_SUFFIX
        + output_suffix
    )


def source_from_output_channel(channel, output_prefix, output_suffix, expected_sources):
    composed_prefix = output_prefix + FEED_PREFIX
    composed_suffix = FEED_SUFFIX + output_suffix
    assert channel.startswith(composed_prefix), channel
    assert channel.endswith(composed_suffix), channel
    source_channel = channel[
        len(composed_prefix) : -len(composed_suffix)
    ].decode("utf-8")
    assert source_channel in expected_sources, {
        "output_channel": channel,
        "expected_sources": sorted(expected_sources),
    }
    expected_channel = expected_output_channel(
        output_prefix, output_suffix, source_channel
    )
    assert channel == expected_channel, {
        "actual": channel,
        "expected": expected_channel,
    }
    return source_channel


def get_status():
    request = urllib.request.Request(STATUS_URL, method="GET")
    with urllib.request.urlopen(request, timeout=3) as response:
        assert response.status == 200
        return json.loads(response.read())


def wait_for_status(predicate, description, timeout=10):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = get_status()
        if predicate(latest):
            return latest
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}; last status: {latest}")


def output_metrics(status):
    outputs = status.get("outputs")
    assert isinstance(outputs, dict), status
    assert set(outputs) == {OUTPUT0_NAME, OUTPUT1_NAME}, outputs
    assert isinstance(outputs[OUTPUT0_NAME], dict), outputs[OUTPUT0_NAME]
    assert isinstance(outputs[OUTPUT1_NAME], dict), outputs[OUTPUT1_NAME]
    return outputs


def assert_database_selection():
    sock, reader = connect(0)
    send_command(sock, "CLIENT", "LIST")
    client_list = read_response(reader)
    close(sock, reader)

    clients = [
        dict(field.split("=", 1) for field in line.split() if "=" in field)
        for line in client_list.decode("utf-8").splitlines()
    ]
    database_zero_clients = [client for client in clients if client.get("db") == "0"]

    assert any("P" in client.get("flags", "") for client in database_zero_clients), (
        "the service input Pub/Sub connection is not using DB0"
    )
    assert sum("P" not in client.get("flags", "") for client in database_zero_clients) >= 3, (
        "DB0 should have the random publisher and output0 connections"
    )
    assert any(
        client.get("db") == "1" and "P" not in client.get("flags", "")
        for client in clients
    ), "output1 is not using DB1"


def collect_output_messages(
    output0_sock,
    output0_reader,
    output1_sock,
    output1_reader,
    feed_publications,
    expected_latest,
    timeout=15,
    quiet_time=1.0,
):
    output0_messages = []
    output1_messages = []
    output1_latest = {}
    expected_sources = set(expected_latest)
    deadline = time.monotonic() + timeout
    quiet_deadline = None
    quiet_completed = False

    while time.monotonic() < deadline:
        readable, _, _ = select.select(
            [output0_sock, output1_sock], [], [], 0.1
        )
        if not readable:
            if quiet_deadline is not None and time.monotonic() >= quiet_deadline:
                quiet_completed = True
                break
            continue

        for sock in readable:
            if sock is output0_sock:
                channel, payload = read_output_message(sock, output0_reader)
                source_channel = source_from_output_channel(
                    channel, OUTPUT0_PREFIX, OUTPUT0_SUFFIX, expected_sources
                )
                output0_messages.append((channel, payload))
            else:
                channel, payload = read_output_message(sock, output1_reader)
                source_channel = source_from_output_channel(
                    channel, OUTPUT1_PREFIX, OUTPUT1_SUFFIX, expected_sources
                )
                output1_messages.append((channel, payload))
                output1_latest[source_channel] = payload

        if (
            len(output0_messages) >= len(feed_publications)
            and set(output1_latest) == expected_sources
        ):
            quiet_deadline = time.monotonic() + quiet_time

    assert quiet_completed, "outputs did not become quiet; a Pub/Sub loop may be active"
    expected_output0 = Counter(
        (
            expected_output_channel(OUTPUT0_PREFIX, OUTPUT0_SUFFIX, channel),
            payload,
        )
        for channel, payload in feed_publications
    )
    assert Counter(output0_messages) == expected_output0, {
        "actual_output0_count": len(output0_messages),
        "expected_output0_count": len(feed_publications),
    }

    expected_output1 = {
        expected_output_channel(OUTPUT1_PREFIX, OUTPUT1_SUFFIX, channel): payload
        for channel, payload in expected_latest.items()
    }
    assert len(output1_messages) == len(expected_latest), {
        "actual_output1_count": len(output1_messages),
        "expected_output1_count": len(expected_latest),
    }
    assert dict(output1_messages) == expected_output1, {
        "actual": dict(output1_messages),
        "expected": expected_output1,
    }
    assert output1_latest == expected_latest, {
        "actual": output1_latest,
        "expected": expected_latest,
    }
    return output0_messages, output1_messages


def main():
    control_sock, control_reader = connect(0)
    send_command(control_sock, "SUBSCRIBE", DONE_CHANNEL)
    assert read_response(control_reader)[0] == b"subscribe"

    wait_for_input_subscriptions()
    wait_for_random_publisher()

    output0_sock, output0_reader = connect(0)
    send_command(output0_sock, "PSUBSCRIBE", OUTPUT0_PATTERN)
    assert read_response(output0_reader)[0] == b"psubscribe"
    output1_sock, output1_reader = connect(1)
    send_command(output1_sock, "PSUBSCRIBE", OUTPUT1_PATTERN)
    assert read_response(output1_reader)[0] == b"psubscribe"

    # Let the initial output1 interval tick pass so the rapid feed burst fits one flush.
    time.sleep(0.35)

    starter_sock, starter_reader = connect(0)
    send_command(starter_sock, "PUBLISH", START_CHANNEL, b"publish")
    assert read_response(starter_reader) >= 1
    close(starter_sock, starter_reader)

    done_message = read_response(control_reader)
    assert (
        done_message[0] == b"message"
        and done_message[1] == DONE_CHANNEL.encode()
        and done_message[2] == b"done"
    ), done_message
    feed_publications, expected_latest, _ = make_publications()
    published_feed_count = len(feed_publications)
    assert {channel for channel, _ in feed_publications} == FEED_CHANNELS
    assert published_feed_count > len(FEED_CHANNELS)
    assert expected_latest["test-feed:alpha"].startswith(b"\x00\xff")
    assert expected_latest["test-feed:beta"].startswith(b"final-beta:")
    assert expected_latest["test-feed:gamma"].startswith(b"\x00\xff")

    wait_for_status(
        lambda status: status.get("state") == "running"
        and status.get("input_messages_total") == published_feed_count,
        "all raw feed messages to reach the input",
    )

    output0_messages, output1_messages = collect_output_messages(
        output0_sock,
        output0_reader,
        output1_sock,
        output1_reader,
        feed_publications,
        expected_latest,
    )
    assert len(output0_messages) == published_feed_count
    assert len(output1_messages) == len(FEED_CHANNELS)

    output_counts = {
        OUTPUT0_NAME: len(output0_messages),
        OUTPUT1_NAME: len(output1_messages),
    }
    status = wait_for_status(
        lambda current: current.get("state") == "running"
        and current.get("input_messages_total") == published_feed_count
        and current.get("excluded_messages_total")
        == sum(output_counts.values())
        and current.get("dropped_messages_total", 0) == 0
        and output_metrics(current)[OUTPUT0_NAME].get("output_messages_total")
        == output_counts[OUTPUT0_NAME]
        and output_metrics(current)[OUTPUT1_NAME].get("output_messages_total")
        == output_counts[OUTPUT1_NAME]
        and 2
        <= output_metrics(current)[OUTPUT1_NAME].get("output_batches_total", 0)
        <= 3
        and output_metrics(current)[OUTPUT1_NAME].get("publish_errors_total") == 0,
        "the per-output counters and echo filtering to settle",
    )
    metrics = output_metrics(status)
    output0_metrics = metrics[OUTPUT0_NAME]
    output1_metrics = metrics[OUTPUT1_NAME]
    assert output0_metrics["output_batches_total"] >= 1, output0_metrics
    assert output0_metrics["conflated_messages_total"] == 0, output0_metrics
    assert output0_metrics["publish_errors_total"] == 0, output0_metrics
    assert 2 <= output1_metrics["output_batches_total"] <= 3, output1_metrics
    assert output1_metrics["conflated_messages_total"] == (
        published_feed_count - len(output1_messages)
    ), output1_metrics
    assert output1_metrics["publish_errors_total"] == 0, output1_metrics
    assert status["input_messages_total"] == published_feed_count, status
    assert status["excluded_messages_total"] == sum(output_counts.values()), status
    assert status.get("dropped_messages_total", 0) == 0, status

    assert_database_selection()

    readable, _, _ = select.select(
        [output0_sock, output1_sock], [], [], 1.0
    )
    assert not readable, "the service republished its own output or emitted a decoy"
    final_status = get_status()
    assert final_status["input_messages_total"] == published_feed_count, final_status
    assert final_status["excluded_messages_total"] == sum(output_counts.values()), final_status
    assert final_status.get("dropped_messages_total", 0) == 0, final_status
    final_outputs = output_metrics(final_status)
    assert final_outputs[OUTPUT0_NAME]["output_messages_total"] == output_counts[OUTPUT0_NAME]
    assert final_outputs[OUTPUT1_NAME]["output_messages_total"] == output_counts[OUTPUT1_NAME]
    assert 2 <= final_outputs[OUTPUT1_NAME]["output_batches_total"] <= 3
    assert final_outputs[OUTPUT1_NAME]["publish_errors_total"] == 0

    close(control_sock, control_reader)
    close(output0_sock, output0_reader)
    close(output1_sock, output1_reader)
    print(
        "Docker Redis Pub/Sub test passed: DB0 feed -> output0 on DB0 forwarded "
        "every raw message; output1 on DB1 emitted the latest value per source "
        "across multiple byte/command-limited MULTI/EXEC batches; decoys and output "
        "echoes were ignored."
    )


if __name__ == "__main__":
    main()
