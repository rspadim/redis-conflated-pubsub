import json
import math
import os
import select
import socket
import threading
import time
import urllib.request

from redis_resp import close, connect, read_response, send_command


OUTPUT_HOST = os.environ.get("OUTPUT_REDIS_HOST", "redis-output")
STATUS_URL = os.environ.get("STATUS_URL", "http://service-hotpath:9090/")
PUBLISHERS = int(os.environ.get("PUBLISHER_COUNT", "4"))
MESSAGES_PER_PUBLISHER = int(os.environ.get("MESSAGES_PER_PUBLISHER", "2500"))
SERIAL_MESSAGES = int(os.environ.get("SERIAL_MESSAGES", "250"))
OUTPUT_NAMES = ("out-a", "out-b")
OUTPUT_PREFIXES = (b"replica-a:", b"replica-b:")
HOT_CHANNEL_COUNT = 64


def connect_output(database):
    sock = socket.create_connection((OUTPUT_HOST, 6379), timeout=10)
    sock.settimeout(30)
    # Buffered reads: the concurrent phase reads every message, and the
    # unbuffered variant paid a syscall per RESP fragment under the GIL.
    reader = sock.makefile("rb")
    send_command(sock, "SELECT", database)
    assert read_response(reader) == "OK"
    return sock, reader


def wait_for_input_subscription(timeout=15):
    deadline = time.monotonic() + timeout
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
    raise AssertionError(f"Bridge input pattern did not become active; NUMPAT={observed}")


def source_channel(channel_index):
    return f"hot:tenant-{channel_index % 16:02}:item-{channel_index:04}".encode()


def output_channel(output_index, channel_index):
    return (
        OUTPUT_PREFIXES[output_index]
        + b"mapped:"
        + source_channel(channel_index)
        + b":source:dest"
    )


def open_output_subscriber(output_index):
    sock, reader = connect_output(output_index)
    pattern = OUTPUT_PREFIXES[output_index] + b"*"
    send_command(sock, "PSUBSCRIBE", pattern)
    acknowledgement = read_response(reader)
    assert acknowledgement[0] == b"psubscribe", acknowledgement
    return sock, reader, pattern


def receive_expected(sock, reader, pattern, channel, payload, timeout=5):
    readable, _, _ = select.select([sock], [], [], timeout)
    assert readable, f"Timed out waiting for {channel!r}"
    message = read_response(reader)
    assert isinstance(message, list) and message[0] == b"pmessage", message
    assert message[1] == pattern, message
    assert message[2:] == [channel, payload], message


def warm_caches(publisher_sock, publisher_reader, subscribers):
    for channel_index in range(HOT_CHANNEL_COUNT):
        channel = source_channel(channel_index)
        payload = f"warm:{channel_index}".encode()
        send_command(publisher_sock, "PUBLISH", channel, payload)
        assert read_response(publisher_reader) >= 1
        for output_index, subscriber in enumerate(subscribers):
            sock, reader, pattern = subscriber
            receive_expected(
                sock,
                reader,
                pattern,
                output_channel(output_index, channel_index),
                payload,
            )


def percentile(sorted_values, percent):
    index = max(0, math.ceil(percent * len(sorted_values)) - 1)
    return sorted_values[index] / 1_000_000


def wait_for_status_totals(expected, timeout=10):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        with urllib.request.urlopen(STATUS_URL, timeout=5) as response:
            assert response.status == 200
            latest = json.loads(response.read())
        outputs = latest.get("outputs", {})
        if (
            latest.get("state") == "running"
            and latest.get("input_messages_total") == expected
            and all(
                outputs.get(name, {}).get("output_messages_total") == expected
                and outputs[name].get("pending_messages") == 0
                and outputs[name].get("publish_errors_total") == 0
                for name in OUTPUT_NAMES
            )
        ):
            return latest
        time.sleep(0.02)
    raise AssertionError(f"Output workers did not flush all messages; last status: {latest}")


def print_output_totals(status):
    for name in OUTPUT_NAMES:
        output = status["outputs"][name]
        queue_samples = output["queue_wait_samples"]
        publish_samples = output["publish_rtt_samples"]
        queue_wait_avg_ms = (
            output["queue_wait_total_ns"] / queue_samples / 1_000_000
            if queue_samples
            else 0.0
        )
        publish_rtt_avg_ms = (
            output["publish_rtt_total_ns"] / publish_samples / 1_000_000
            if publish_samples
            else 0.0
        )
        print(
            f"output {name} input={output['input_messages_total']} "
            f"published={output['output_messages_total']} batches={output['output_batches_total']} "
            f"pending={output['pending_messages']} "
            f"queue_wait_avg_max_ms={queue_wait_avg_ms:.3f}/"
            f"{output['queue_wait_max_ns'] / 1_000_000:.3f} "
            f"publish_rtt_avg_max_ms={publish_rtt_avg_ms:.3f}/"
            f"{output['publish_rtt_max_ns'] / 1_000_000:.3f}"
        )


def main():
    expected = PUBLISHERS * MESSAGES_PER_PUBLISHER
    wait_for_input_subscription()

    subscribers = [open_output_subscriber(index) for index in range(2)]
    publisher_sock, publisher_reader = connect(0)
    try:
        warm_caches(publisher_sock, publisher_reader, subscribers)

        serial_latencies_ns = []
        for sequence in range(SERIAL_MESSAGES):
            channel_index = sequence % HOT_CHANNEL_COUNT
            payload = f"serial:{sequence}".encode()
            started = time.perf_counter_ns()
            send_command(publisher_sock, "PUBLISH", source_channel(channel_index), payload)
            assert read_response(publisher_reader) >= 1
            for output_index, subscriber in enumerate(subscribers):
                sock, reader, pattern = subscriber
                receive_expected(
                    sock,
                    reader,
                    pattern,
                    output_channel(output_index, channel_index),
                    payload,
                )
            serial_latencies_ns.append(time.perf_counter_ns() - started)
        serial_sorted = sorted(serial_latencies_ns)
        print(
            "hotpath-serial "
            f"messages={SERIAL_MESSAGES} cache_mode=worker-local "
            f"latency_ms_p50={percentile(serial_sorted, 0.50):.3f} "
            f"p95={percentile(serial_sorted, 0.95):.3f} "
            f"p99={percentile(serial_sorted, 0.99):.3f} "
            f"max={serial_sorted[-1] / 1_000_000:.3f}"
        )

        start_barrier = threading.Barrier(PUBLISHERS + 1)
        failures = []
        sent = {}
        sent_lock = threading.Lock()
        received = [{}, {}]
        timeline = []
        stop_sampling = threading.Event()

        def consume_output(output_index, subscriber):
            sock, reader, pattern = subscriber
            local = {}
            try:
                for _ in range(expected):
                    message = read_response(reader)
                    assert isinstance(message, list) and message[0] == b"pmessage", message
                    assert message[1] == pattern, message
                    publisher_id, sequence, _sent_at = map(int, message[3].split(b":"))
                    t_recv = time.perf_counter_ns()
                    expected_channel = output_channel(
                        output_index,
                        (publisher_id * MESSAGES_PER_PUBLISHER + sequence) % HOT_CHANNEL_COUNT,
                    )
                    assert message[2] == expected_channel, (message[2], expected_channel)
                    local[(publisher_id, sequence)] = t_recv
            except Exception as error:  # surfaced in the test thread after joins
                failures.append((f"subscriber-{output_index}", repr(error)))
            finally:
                received[output_index] = local

        def publish_messages(publisher_id):
            sock, reader = connect(0)
            local = {}
            try:
                start_barrier.wait()
                for sequence in range(MESSAGES_PER_PUBLISHER):
                    channel_index = (
                        publisher_id * MESSAGES_PER_PUBLISHER + sequence
                    ) % HOT_CHANNEL_COUNT
                    t0 = time.perf_counter_ns()
                    send_command(
                        sock,
                        "PUBLISH",
                        source_channel(channel_index),
                        f"{publisher_id}:{sequence}:{t0}",
                    )
                    assert read_response(reader) >= 1
                    local[(publisher_id, sequence)] = (t0, time.perf_counter_ns())
            except Exception as error:
                failures.append((f"publisher-{publisher_id}", repr(error)))
            finally:
                with sent_lock:
                    sent.update(local)
                close(sock, reader)

        def sample_status():
            while not stop_sampling.is_set():
                try:
                    with urllib.request.urlopen(STATUS_URL, timeout=5) as response:
                        status = json.loads(response.read())
                    timeline.append(
                        (
                            time.perf_counter(),
                            status.get("input_messages_total", 0),
                            status["outputs"]["out-a"].get("output_messages_total", 0),
                            status["outputs"]["out-b"].get("output_messages_total", 0),
                        )
                    )
                except Exception:
                    pass
                stop_sampling.wait(0.1)

        output_threads = [
            threading.Thread(target=consume_output, args=(index, subscriber), daemon=True)
            for index, subscriber in enumerate(subscribers)
        ]
        for thread in output_threads:
            thread.start()

        sampler_thread = threading.Thread(target=sample_status, daemon=True)
        sampler_thread.start()

        publisher_threads = [
            threading.Thread(target=publish_messages, args=(index,), daemon=True)
            for index in range(PUBLISHERS)
        ]
        for thread in publisher_threads:
            thread.start()

        wall_started = time.perf_counter()
        start_barrier.wait()
        for thread in publisher_threads:
            thread.join(timeout=60)
            assert not thread.is_alive(), "publisher thread timed out"
        publishers_done = time.perf_counter()
        for thread in output_threads:
            thread.join(timeout=60)
            assert not thread.is_alive(), "output subscriber did not receive all messages"
        wall_finished = time.perf_counter()
        stop_sampling.set()
        sampler_thread.join(timeout=5)
        publisher_elapsed = publishers_done - wall_started
        wall_elapsed = wall_finished - wall_started
        drain_elapsed = wall_finished - publishers_done

        assert not failures, failures[:3]
        assert len(sent) == expected, (len(sent), expected)
        assert len(received[0]) == expected, (len(received[0]), expected)
        assert len(received[1]) == expected, (len(received[1]), expected)

        input_rtt_ns = [t_ret - t0 for t0, t_ret in sent.values()]
        e2e_ns = [[], []]
        post_ack_ns = [[], []]
        for key, (t0, t_ret) in sent.items():
            for output_index in range(2):
                t_recv = received[output_index][key]
                e2e_ns[output_index].append(t_recv - t0)
                post_ack_ns[output_index].append(t_recv - t_ret)

        def reached_at(check, start):
            for sample_at, input_total, out_a_total, out_b_total in timeline:
                if check(input_total, out_a_total, out_b_total):
                    return sample_at - start
            return float("nan")

        input_rtt_sorted = sorted(input_rtt_ns)
        total_expected = expected + HOT_CHANNEL_COUNT + SERIAL_MESSAGES
        print(
            "hotpath-e2e "
            f"publishers={PUBLISHERS} messages_per_publisher={MESSAGES_PER_PUBLISHER} "
            f"outputs=2 cache_mode=worker-local hot_channels={HOT_CHANNEL_COUNT} "
            f"warmup={HOT_CHANNEL_COUNT} serial={SERIAL_MESSAGES} "
            f"messages={expected} publish_phase_s={publisher_elapsed:.3f} "
            f"output_drain_s={drain_elapsed:.3f} elapsed_s={wall_elapsed:.3f} "
            f"end_to_end_messages_per_second={expected / wall_elapsed:.0f} "
            f"input_ack_rtt_p50_p95_ms={percentile(input_rtt_sorted, 0.50):.3f}/"
            f"{percentile(input_rtt_sorted, 0.95):.3f}"
        )
        print(
            "hotpath-timeline "
            f"subscribers_done_s={wall_finished - wall_started:.3f} "
            f"input_done_s={reached_at(lambda i, a, b: i >= total_expected, wall_started):.3f} "
            f"out-a_done_s={reached_at(lambda i, a, b: a >= total_expected, wall_started):.3f} "
            f"out-b_done_s={reached_at(lambda i, a, b: b >= total_expected, wall_started):.3f}"
        )
        for output_index, name in enumerate(OUTPUT_NAMES):
            e2e_sorted = sorted(e2e_ns[output_index])
            post_ack_sorted = sorted(post_ack_ns[output_index])
            print(
                f"hotpath-phase {name} "
                f"e2e_p50_p95_p99_ms={percentile(e2e_sorted, 0.50):.3f}/"
                f"{percentile(e2e_sorted, 0.95):.3f}/"
                f"{percentile(e2e_sorted, 0.99):.3f} "
                f"post_ack_p50_p95_ms={percentile(post_ack_sorted, 0.50):.3f}/"
                f"{percentile(post_ack_sorted, 0.95):.3f}"
            )
        status = wait_for_status_totals(expected + HOT_CHANNEL_COUNT + SERIAL_MESSAGES)
        print_output_totals(status)
    finally:
        close(publisher_sock, publisher_reader)
        for sock, reader, _ in subscribers:
            close(sock, reader)


if __name__ == "__main__":
    main()
