import glob
import json
import os
import select
import signal
import time
import urllib.request

from redis_resp import close, connect, read_response, send_command


STATUS_URL = os.environ.get("STATUS_URL", "http://service-ttl:9090/")
SOURCE_CHANNEL = b"ttl-feed:binary"
TTL_MS = 600
CONFLATION_INTERVAL_MS = 50
FLOOR_GROUP_TTL_MS = 2600
FLOOR_GROUP_ROUND_MS = 1000
FLOOR_GROUP_GUARD_MS = 350
PAYLOAD_A = b"\x00\xffttl-A:\x80\r\n\x00"
PAYLOAD_B = b"\xff\x00ttl-B:\xfe\r\n"
PAYLOAD_C = b"\x7f\x00ttl-C:\xfd\r\n"
OUTPUT_CHANNELS = {
    "ttl": b"ttl-out:ttl-feed:binary",
    "window-zero": b"window-zero-out:ttl-feed:binary",
    "direct-zero": b"direct-zero-out:ttl-feed:binary",
}


def wait_for_input_subscription():
    deadline = time.monotonic() + 15
    observed = None
    while time.monotonic() < deadline:
        sock, reader = connect(0)
        try:
            send_command(sock, "PUBSUB", "NUMPAT")
            observed = read_response(reader)
        finally:
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


def wait_for_sighup_reload(publisher_sock, publisher_reader, timeout=15):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        try:
            latest = get_status()
        except OSError:
            time.sleep(0.05)
            continue
        if latest.get("state") == "running" and latest.get("input_messages_total") == 0:
            send_command(publisher_sock, "PUBSUB", "NUMPAT")
            if read_response(publisher_reader) == 4:
                return latest
        time.sleep(0.05)
    raise AssertionError(f"Service did not finish SIGHUP reload; last status: {latest}")


def wait_for_rejected_reload(timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for log_path in glob.glob("/state/logs/redis-conflated-pubsub.*"):
            try:
                with open(log_path, encoding="utf-8") as log_file:
                    if "configuration_reload_rejected" in log_file.read():
                        return
            except FileNotFoundError:
                continue
        time.sleep(0.05)
    raise AssertionError("Service did not log rejection of the invalid SIGHUP config")


def exercise_sighup_reload(publisher_sock, publisher_reader, subscribers):
    service_pid = os.environ.get("SERVICE_PID")
    if not service_pid:
        return

    config_path = "/state/docker-ttl-config.json"
    with open(config_path, "rb") as config_file:
        original_config = config_file.read()
    updated_config = json.loads(original_config)
    updated_config["outputs"]["ttl"]["profiles"]["default"][
        "deduplication.ttl_ms"
    ] = 1200

    try:
        with open(config_path, "w", encoding="utf-8") as config_file:
            json.dump(updated_config, config_file)
        os.kill(int(service_pid), signal.SIGHUP)
        wait_for_sighup_reload(publisher_sock, publisher_reader)
        probe = "reload-probe"
        publish_policy_event(
            publisher_sock,
            publisher_reader,
            subscribers,
            probe,
            PAYLOAD_A,
            ttl_expected=True,
        )
        time.sleep(0.85)
        publish_policy_batch(
            publisher_sock,
            publisher_reader,
            subscribers,
            [(probe, PAYLOAD_A)],
            ttl_items=[],
            assert_ttl_quiet=True,
        )

        invalid_probe = "reload-invalid-probe"
        publish_policy_event(
            publisher_sock,
            publisher_reader,
            subscribers,
            invalid_probe,
            PAYLOAD_B,
            ttl_expected=True,
        )
        time.sleep(0.85)
        with open(config_path, "w", encoding="utf-8") as config_file:
            config_file.write("{ invalid json")
        os.kill(int(service_pid), signal.SIGHUP)
        wait_for_rejected_reload()
        status = get_status()
        assert status.get("state") == "running", status
        os.kill(int(service_pid), 0)
        # The valid 1200 ms TTL remains active. If the rejected reload had
        # replaced it with the original 600 ms config, this duplicate publishes.
        publish_policy_batch(
            publisher_sock,
            publisher_reader,
            subscribers,
            [(invalid_probe, PAYLOAD_B)],
            ttl_items=[],
            assert_ttl_quiet=True,
        )
    finally:
        with open(config_path, "wb") as config_file:
            config_file.write(original_config)


def open_subscribers():
    subscribers = {}
    for index, name in enumerate(("ttl", "window-zero", "direct-zero"), start=1):
        sock, reader = connect(index)
        send_command(sock, "PSUBSCRIBE", f"{name}-out:*")
        acknowledgement = read_response(reader)
        assert acknowledgement[0] == b"psubscribe", acknowledgement
        subscribers[name] = (sock, reader)
    return subscribers


def publish(sock, reader, payload, channel=SOURCE_CHANNEL):
    send_command(sock, "PUBLISH", channel, payload)
    subscriber_count = read_response(reader)
    assert subscriber_count >= 1, (channel, subscriber_count)
    return time.monotonic()


def receive_message(subscriber, timeout=10):
    sock, reader = subscriber
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        readable, _, _ = select.select([sock], [], [], min(0.1, deadline - time.monotonic()))
        if readable:
            message = read_response(reader)
            assert isinstance(message, list) and message[0] == b"pmessage", message
            return (message[2], message[3])
    raise AssertionError("Timed out waiting for a Pub/Sub message")


def receive_expected(subscriber, expected, timeout=10):
    actual = receive_message(subscriber, timeout)
    assert actual == expected, {"expected": expected, "actual": actual}
    return actual


def receive_many(subscriber, expected, timeout=10):
    pending = dict(expected)
    received_at = {}
    deadline = time.monotonic() + timeout
    while pending:
        remaining = deadline - time.monotonic()
        assert remaining > 0, f"Timed out waiting for Pub/Sub messages {pending!r}"
        channel, payload = receive_message(subscriber, remaining)
        assert channel in pending, {
            "unexpected_channel": channel,
            "expected_channels": sorted(pending),
        }
        assert payload == pending[channel], {
            "channel": channel,
            "expected_payload": pending[channel],
            "actual_payload": payload,
        }
        received_at[channel] = time.monotonic()
        del pending[channel]
    return received_at


def mapped_channel(output_name, source_name):
    return f"{output_name}-out:ttl-feed:{source_name}".encode()


def publish_policy_batch(
    publisher_sock,
    publisher_reader,
    subscribers,
    items,
    ttl_items=None,
    ttl_timeout=10,
    assert_ttl_quiet=False,
):
    source_channels = [
        (f"ttl-feed:{source_name}".encode(), payload)
        for source_name, payload in items
    ]
    for channel, payload in source_channels:
        send_command(publisher_sock, "PUBLISH", channel, payload)
    for channel, _ in source_channels:
        subscriber_count = read_response(publisher_reader)
        assert subscriber_count >= 1, (channel, subscriber_count)

    for (source_name, payload), (channel, _) in zip(items, source_channels):
        receive_expected(
            subscribers["direct-zero"],
            (mapped_channel("direct-zero", source_name), payload),
        )

    ttl_times = {}
    if ttl_items:
        ttl_times = receive_many(
            subscribers["ttl"],
            {
                mapped_channel("ttl", source_name): payload
                for source_name, payload in ttl_items
            },
            timeout=ttl_timeout,
        )
    if assert_ttl_quiet:
        assert_quiet({"ttl": subscribers["ttl"]}, duration=0.12)

    window_zero_expected = {
        mapped_channel("window-zero", source_name): payload
        for source_name, payload in items
    }
    window_zero_times = receive_many(
        subscribers["window-zero"], window_zero_expected
    )
    return ttl_times, window_zero_times


def assert_quiet(subscribers, duration=0.5):
    deadline = time.monotonic() + duration
    sockets = [sock for sock, _ in subscribers.values()]
    readers = {sock: reader for sock, reader in subscribers.values()}
    while time.monotonic() < deadline:
        readable, _, _ = select.select(sockets, [], [], min(0.1, deadline - time.monotonic()))
        if readable:
            sock = readable[0]
            raise AssertionError(f"Unexpected duplicate Pub/Sub event: {read_response(readers[sock])!r}")


def next_window_start(last_flush_at, interval_ms, guard_ms=2):
    interval = interval_ms / 1000
    now = time.monotonic()
    periods = max(1, int((now - last_flush_at) / interval) + 1)
    target = last_flush_at + periods * interval + guard_ms / 1000
    time.sleep(max(0, target - now))
    return time.monotonic()


def wait_for_next_windows(anchors, guard_ms=20):
    now = time.monotonic()
    targets = []
    for last_flush_at, interval_ms in anchors:
        interval = interval_ms / 1000
        periods = max(1, int((now - last_flush_at) / interval) + 1)
        targets.append(last_flush_at + periods * interval + guard_ms / 1000)
    target = max(targets)
    time.sleep(max(0, target - now))
    return time.monotonic()


def wait_for_epoch_phase(round_ms, minimum_ms, maximum_ms, timeout=4):
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        now_epoch = time.time()
        phase_ms = (now_epoch * 1000) % round_ms
        if minimum_ms <= phase_ms <= maximum_ms:
            return now_epoch
        if phase_ms < minimum_ms:
            delay = (minimum_ms - phase_ms) / 1000
        else:
            delay = (round_ms - phase_ms + minimum_ms) / 1000
        time.sleep(min(delay, remaining))
    raise AssertionError(
        f"Could not reach {minimum_ms}-{maximum_ms} ms phase of {round_ms} ms"
    )


def publish_policy_event(
    publisher_sock, publisher_reader, subscribers, source_name, payload, ttl_expected
):
    ttl_items = [(source_name, payload)] if ttl_expected else []
    ttl_times, _ = publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        [(source_name, payload)],
        ttl_items=ttl_items,
        assert_ttl_quiet=not ttl_expected,
    )
    if ttl_expected:
        return ttl_times[mapped_channel("ttl", source_name)]
    return None


def exercise_channel_policies(publisher_sock, publisher_reader, subscribers):
    reuse_items = [
        ("profile-reuse-a", PAYLOAD_A),
        ("profile-reuse-b", PAYLOAD_A),
    ]
    reuse_initial, _ = publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        reuse_items,
        ttl_items=reuse_items,
    )
    publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        reuse_items,
        ttl_items=[],
        assert_ttl_quiet=True,
    )
    reuse_expiry = max(reuse_initial.values()) + 0.35
    time.sleep(max(0, reuse_expiry - time.monotonic()))
    publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        reuse_items,
        ttl_items=reuse_items,
    )

    ungrouped_early = "ungrouped-early"
    ungrouped_late = "ungrouped-late"
    early_at = publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        ungrouped_early,
        PAYLOAD_A,
        ttl_expected=True,
    )
    time.sleep(0.25)
    publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        ungrouped_late,
        PAYLOAD_A,
        ttl_expected=True,
    )
    ungrouped_expiry = early_at + 0.5
    time.sleep(max(0, ungrouped_expiry - time.monotonic()))
    publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        [(ungrouped_early, PAYLOAD_A), (ungrouped_late, PAYLOAD_A)],
        ttl_items=[(ungrouped_early, PAYLOAD_A)],
        assert_ttl_quiet=True,
    )

    precedence_name = "precedence-suffix"
    precedence_at = publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        precedence_name,
        PAYLOAD_A,
        ttl_expected=True,
    )
    precedence_expiry = precedence_at + 0.35
    time.sleep(max(0, precedence_expiry - time.monotonic()))
    publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        precedence_name,
        PAYLOAD_A,
        ttl_expected=True,
    )

    selector_items = [
        ("policy-prefix-case", PAYLOAD_A),
        ("namespaced:suffix-inline", PAYLOAD_A),
    ]
    selector_initial, _ = publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        selector_items,
        ttl_items=selector_items,
    )
    selector_expiry = max(selector_initial.values()) + 0.35
    time.sleep(max(0, selector_expiry - time.monotonic()))
    publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        selector_items,
        ttl_items=selector_items,
    )

    fixed_initial_name = "group-fixed-a"
    fixed_changed_name = "group-fixed-b"
    fixed_initial_at = publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        fixed_initial_name,
        PAYLOAD_A,
        ttl_expected=True,
    )
    time.sleep(max(0, fixed_initial_at + 0.25 - time.monotonic()))
    publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        fixed_changed_name,
        PAYLOAD_B,
        ttl_expected=True,
    )
    time.sleep(max(0, fixed_initial_at + 0.7 - time.monotonic()))
    publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        fixed_changed_name,
        PAYLOAD_B,
        ttl_expected=True,
    )

    reset_initial_name = "group-reset-a"
    reset_changed_name = "group-reset-b"
    reset_initial_at = publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        reset_initial_name,
        PAYLOAD_A,
        ttl_expected=True,
    )
    time.sleep(max(0, reset_initial_at + 0.5 - time.monotonic()))
    reset_changed_at = publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        reset_changed_name,
        PAYLOAD_B,
        ttl_expected=True,
    )
    time.sleep(max(0, reset_changed_at + 0.35 - time.monotonic()))
    publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        reset_initial_name,
        PAYLOAD_A,
        ttl_expected=False,
    )
    time.sleep(max(0, reset_changed_at + 0.85 - time.monotonic()))
    publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        reset_initial_name,
        PAYLOAD_A,
        ttl_expected=True,
    )

    wait_for_epoch_phase(FLOOR_GROUP_ROUND_MS, 550, 650)
    floor_name = "group-floor-a"
    floor_first_at = publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        floor_name,
        PAYLOAD_A,
        ttl_expected=True,
    )
    floor_phase_ms = (time.time() * 1000) % FLOOR_GROUP_ROUND_MS
    assert 450 <= floor_phase_ms <= 750, {
        "floor_group_publish_phase_ms": floor_phase_ms,
    }
    floor_expiry_at = (
        floor_first_at * 1000
        - floor_phase_ms
        + FLOOR_GROUP_TTL_MS
    ) / 1000
    before_floor_expiry_at = floor_expiry_at - FLOOR_GROUP_GUARD_MS / 1000
    time.sleep(max(0, before_floor_expiry_at - time.monotonic()))
    publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        floor_name,
        PAYLOAD_A,
        ttl_expected=False,
    )
    after_floor_expiry_at = floor_expiry_at + FLOOR_GROUP_GUARD_MS / 1000
    time.sleep(max(0, after_floor_expiry_at - time.monotonic()))
    publish_policy_event(
        publisher_sock,
        publisher_reader,
        subscribers,
        floor_name,
        PAYLOAD_A,
        ttl_expected=True,
    )

    interval_seed = [
        ("interval-fast", PAYLOAD_A),
        ("interval-slow", PAYLOAD_A),
    ]
    interval_initial_times, _ = publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        interval_seed,
        ttl_items=interval_seed,
        ttl_timeout=3,
    )
    interval_fast_channel = mapped_channel("ttl", "interval-fast")
    interval_slow_channel = mapped_channel("ttl", "interval-slow")
    wait_for_next_windows(
        [
            (interval_initial_times[interval_fast_channel], 100),
            (interval_initial_times[interval_slow_channel], 300),
        ]
    )

    interval_followup = [
        ("interval-fast", PAYLOAD_B),
        ("interval-slow", PAYLOAD_C),
    ]
    publish_policy_batch(
        publisher_sock,
        publisher_reader,
        subscribers,
        interval_followup,
        ttl_items=[],
    )
    fast_message = receive_message(subscribers["ttl"], timeout=0.3)
    assert fast_message == (interval_fast_channel, PAYLOAD_B), {
        "expected_fast_interval_message": (interval_fast_channel, PAYLOAD_B),
        "actual": fast_message,
    }
    fast_output_at = time.monotonic()
    assert_quiet({"ttl": subscribers["ttl"]}, duration=0.12)
    slow_message = receive_message(subscribers["ttl"], timeout=0.5)
    assert slow_message == (interval_slow_channel, PAYLOAD_C), {
        "expected_slow_interval_message": (interval_slow_channel, PAYLOAD_C),
        "actual": slow_message,
    }
    slow_output_at = time.monotonic()
    assert slow_output_at - fast_output_at >= 0.12, {
        "fast_output_at": fast_output_at,
        "slow_output_at": slow_output_at,
    }
    assert_quiet(subscribers, duration=0.45)


def main():
    wait_for_input_subscription()
    subscribers = open_subscribers()
    publisher_sock, publisher_reader = connect(0)
    try:
        time.sleep(0.1)
        publish(publisher_sock, publisher_reader, PAYLOAD_A)
        receive_expected(subscribers["ttl"], (OUTPUT_CHANNELS["ttl"], PAYLOAD_A))
        receive_expected(
            subscribers["window-zero"],
            (OUTPUT_CHANNELS["window-zero"], PAYLOAD_A),
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_A),
        )

        first_a_output_at = time.monotonic()
        duplicate_a_sent_at = publish(publisher_sock, publisher_reader, PAYLOAD_A)
        assert duplicate_a_sent_at - first_a_output_at < TTL_MS / 1000, (
            "Repeated A must be published within the configured TTL"
        )
        receive_expected(
            subscribers["window-zero"],
            (OUTPUT_CHANNELS["window-zero"], PAYLOAD_A),
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_A),
        )
        wait_for_status(
            lambda current: current.get("input_messages_total") == 2
            and current.get("outputs", {}).get("ttl", {}).get(
                "deduplicated_messages_total"
            )
            == 1
            and current["outputs"]["ttl"].get("output_messages_total") == 1
            and current["outputs"]["ttl"].get("pending_messages") == 0
            and current["outputs"]["window-zero"].get("output_messages_total") == 2,
            "the repeated A payload to be deduplicated",
        )

        changed_a_to_b_at = publish(publisher_sock, publisher_reader, PAYLOAD_B)
        assert changed_a_to_b_at - duplicate_a_sent_at < TTL_MS / 1000, (
            "Changed B must be accepted within A's configured TTL"
        )
        receive_expected(subscribers["ttl"], (OUTPUT_CHANNELS["ttl"], PAYLOAD_B))
        first_b_output_at = time.monotonic()
        receive_expected(
            subscribers["window-zero"],
            (OUTPUT_CHANNELS["window-zero"], PAYLOAD_B),
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_B),
        )

        duplicate_b_sent_at = publish(publisher_sock, publisher_reader, PAYLOAD_B)
        assert duplicate_b_sent_at - first_b_output_at < TTL_MS / 1000, (
            "Repeated B must be published within the configured TTL"
        )
        receive_expected(
            subscribers["window-zero"],
            (OUTPUT_CHANNELS["window-zero"], PAYLOAD_B),
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_B),
        )
        wait_for_status(
            lambda current: current.get("input_messages_total") == 4
            and current.get("outputs", {}).get("ttl", {}).get(
                "deduplicated_messages_total"
            )
            == 2
            and current["outputs"]["ttl"].get("output_messages_total") == 2
            and current["outputs"]["ttl"].get("pending_messages") == 0
            and current["outputs"]["window-zero"].get("output_messages_total") == 4,
            "the repeated B payload to be deduplicated",
        )

        expiry_deadline = first_b_output_at + (TTL_MS + 300) / 1000
        time.sleep(max(0, expiry_deadline - time.monotonic()))
        publish(publisher_sock, publisher_reader, PAYLOAD_B)
        receive_expected(subscribers["ttl"], (OUTPUT_CHANNELS["ttl"], PAYLOAD_B))
        receive_expected(
            subscribers["window-zero"],
            (OUTPUT_CHANNELS["window-zero"], PAYLOAD_B),
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_B),
        )

        publish(publisher_sock, publisher_reader, PAYLOAD_A)
        receive_expected(subscribers["ttl"], (OUTPUT_CHANNELS["ttl"], PAYLOAD_A))
        receive_expected(
            subscribers["window-zero"],
            (OUTPUT_CHANNELS["window-zero"], PAYLOAD_A),
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_A),
        )

        same_window_anchor = time.monotonic()
        b_to_a_b_sent_at = publish(publisher_sock, publisher_reader, PAYLOAD_B)
        b_to_a_a_sent_at = publish(publisher_sock, publisher_reader, PAYLOAD_A)
        assert b_to_a_b_sent_at - same_window_anchor < CONFLATION_INTERVAL_MS / 2000, (
            "The B/A burst must start within the new conflation window"
        )
        assert b_to_a_a_sent_at - b_to_a_b_sent_at < CONFLATION_INTERVAL_MS / 2000, (
            "The B/A burst must fit inside one conflation window"
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_B),
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_A),
        )
        receive_expected(
            subscribers["window-zero"],
            (OUTPUT_CHANNELS["window-zero"], PAYLOAD_A),
        )
        window_zero_flush_at = time.monotonic()

        wait_for_status(
            lambda current: current.get("outputs", {}).get("ttl", {}).get(
                "deduplicated_messages_total"
            )
            == 3
            and current["outputs"]["ttl"].get("output_messages_total") == 4
            and current["outputs"]["ttl"].get("pending_messages") == 0
            and current["outputs"]["window-zero"].get("output_messages_total") == 7
            and current["outputs"]["window-zero"].get("pending_messages") == 0,
            "the same-window B/A burst to flush as cached A only on TTL 0",
        )
        assert_quiet(
            {"ttl": subscribers["ttl"]},
            duration=(CONFLATION_INTERVAL_MS + 10) / 1000,
        )
        second_window_anchor = next_window_start(
            window_zero_flush_at, CONFLATION_INTERVAL_MS
        )

        b_to_c_b_sent_at = publish(publisher_sock, publisher_reader, PAYLOAD_B)
        b_to_c_c_sent_at = publish(publisher_sock, publisher_reader, PAYLOAD_C)
        assert b_to_c_b_sent_at - second_window_anchor < CONFLATION_INTERVAL_MS / 2000, (
            "The B/C burst must start in the next conflation window"
        )
        assert b_to_c_c_sent_at - b_to_c_b_sent_at < CONFLATION_INTERVAL_MS / 2000, (
            "The B/C burst must fit inside one conflation window"
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_B),
        )
        receive_expected(
            subscribers["direct-zero"],
            (OUTPUT_CHANNELS["direct-zero"], PAYLOAD_C),
        )
        receive_expected(
            subscribers["window-zero"],
            (OUTPUT_CHANNELS["window-zero"], PAYLOAD_C),
        )
        receive_expected(
            subscribers["ttl"],
            (OUTPUT_CHANNELS["ttl"], PAYLOAD_C),
        )
        assert_quiet(subscribers)

        status = wait_for_status(
            lambda current: current.get("state") == "running"
            and current.get("input_messages_total") == 10
            and set(current.get("outputs", {}))
            == {"ttl", "window-zero", "direct-zero"}
            and all(
                current["outputs"][name].get("output_messages_total") == count
                and current["outputs"][name].get("pending_messages") == 0
                and current["outputs"][name].get("pending_payload_bytes") == 0
                for name, count in (
                    ("ttl", 5),
                    ("window-zero", 8),
                    ("direct-zero", 10),
                )
            ),
            "all TTL, conflated TTL-zero, and direct TTL-zero publications to finish",
        )

        outputs = status["outputs"]
        ttl_metrics = outputs["ttl"]
        window_zero_metrics = outputs["window-zero"]
        direct_zero_metrics = outputs["direct-zero"]
        assert ttl_metrics["deduplicated_messages_total"] == 3, ttl_metrics
        assert ttl_metrics["deduplicated_payload_bytes_total"] == (
            2 * len(PAYLOAD_A) + len(PAYLOAD_B)
        ), ttl_metrics
        for metrics in (window_zero_metrics, direct_zero_metrics):
            assert metrics["deduplicated_messages_total"] == 0, metrics
            assert metrics["deduplicated_payload_bytes_total"] == 0, metrics
        assert ttl_metrics["output_payload_bytes_total"] == (
            2 * len(PAYLOAD_A) + 2 * len(PAYLOAD_B) + len(PAYLOAD_C)
        ), ttl_metrics
        assert window_zero_metrics["output_payload_bytes_total"] == (
            4 * len(PAYLOAD_A) + 3 * len(PAYLOAD_B) + len(PAYLOAD_C)
        ), window_zero_metrics
        assert direct_zero_metrics["output_payload_bytes_total"] == (
            4 * len(PAYLOAD_A) + 5 * len(PAYLOAD_B) + len(PAYLOAD_C)
        ), direct_zero_metrics
        for name, metrics in outputs.items():
            assert metrics["publish_errors_total"] == 0, (name, metrics)
            assert metrics["pending_messages"] == 0, (name, metrics)
            assert metrics["pending_payload_bytes"] == 0, (name, metrics)
            assert metrics["pending_keys"] == 0, (name, metrics)

        exercise_channel_policies(publisher_sock, publisher_reader, subscribers)
        assert_quiet(subscribers, duration=0.45)
        final_status = wait_for_status(
            lambda current: current.get("state") == "running"
            and current.get("input_messages_total") == 40
            and all(
                current.get("outputs", {}).get(name, {}).get(
                    "output_messages_total"
                )
                == count
                and current["outputs"][name].get("pending_messages") == 0
                and current["outputs"][name].get("pending_payload_bytes") == 0
                for name, count in (
                    ("ttl", 30),
                    ("window-zero", 38),
                    ("direct-zero", 40),
                )
            ),
            "all channel-policy TTL and independently scheduled interval cases to finish",
        )
        final_outputs = final_status["outputs"]
        final_ttl_metrics = final_outputs["ttl"]
        assert final_ttl_metrics["deduplicated_messages_total"] == 8, final_ttl_metrics
        assert final_ttl_metrics["deduplicated_payload_bytes_total"] == (
            7 * len(PAYLOAD_A) + len(PAYLOAD_B)
        ), final_ttl_metrics
        assert final_ttl_metrics["output_payload_bytes_total"] == (
            22 * len(PAYLOAD_A) + 6 * len(PAYLOAD_B) + 2 * len(PAYLOAD_C)
        ), final_ttl_metrics
        assert final_outputs["window-zero"]["output_payload_bytes_total"] == (
            29 * len(PAYLOAD_A) + 7 * len(PAYLOAD_B) + 2 * len(PAYLOAD_C)
        ), final_outputs["window-zero"]
        assert final_outputs["direct-zero"]["output_payload_bytes_total"] == (
            29 * len(PAYLOAD_A) + 9 * len(PAYLOAD_B) + 2 * len(PAYLOAD_C)
        ), final_outputs["direct-zero"]
        for name, metrics in final_outputs.items():
            assert metrics["publish_errors_total"] == 0, (name, metrics)
            assert metrics["pending_messages"] == 0, (name, metrics)
            assert metrics["pending_payload_bytes"] == 0, (name, metrics)
            assert metrics["pending_keys"] == 0, (name, metrics)

        exercise_sighup_reload(publisher_sock, publisher_reader, subscribers)

        print(
            "TTL/profile/group E2E passed: profiles, ordered selectors and default, "
            "individual expiry, floor-rounded and fixed/reset group expiry, valid SIGHUP reload, "
            "and rejected invalid-file reload without replacing the active config; "
            "independent 100/300 ms intervals behaved on mapped channels; TTL 0 "
            "still conflated at 50 ms and direct TTL 0 forwarded every input."
        )
    finally:
        close(publisher_sock, publisher_reader)
        for sock, reader in subscribers.values():
            close(sock, reader)

if __name__ == "__main__":
    main()
