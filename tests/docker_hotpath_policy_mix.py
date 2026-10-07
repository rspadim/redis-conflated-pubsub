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
FILTERS_URL = STATUS_URL.rstrip("/") + "/filters"
PUBLISHERS = int(os.environ.get("PUBLISHER_COUNT", "4"))
MESSAGES_PER_PUBLISHER = int(os.environ.get("MESSAGES_PER_PUBLISHER", "2500"))
OUTPUT_NAMES = ("out-a", "out-b")
OUTPUT_PREFIXES = (b"replica-a:", b"replica-b:")
COHORTS = ("direct", "conflate", "ttl")
CHANNELS_PER_COHORT = 16


def connect_output(database):
    sock = socket.create_connection((OUTPUT_HOST, 6379), timeout=10)
    sock.settimeout(30)
    reader = sock.makefile("rb", buffering=0)
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


def source_channel(cohort, channel_index):
    return f"mix:{cohort}:{channel_index:02}".encode()


def output_channel(output_index, cohort, channel_index):
    return (
        OUTPUT_PREFIXES[output_index]
        + b"mapped:mix:"
        + cohort.encode()
        + f":{channel_index:02}:source:dest".encode()
    )


def open_output_subscriber(output_index):
    sock, reader = connect_output(output_index)
    pattern = OUTPUT_PREFIXES[output_index] + b"*"
    send_command(sock, "PSUBSCRIBE", pattern)
    acknowledgement = read_response(reader)
    assert acknowledgement[0] == b"psubscribe", acknowledgement
    return sock, reader, pattern


def warm_caches(publisher_sock, publisher_reader, subscribers):
    expected = {}
    for cohort in COHORTS:
        for channel_index in range(CHANNELS_PER_COHORT):
            channel = source_channel(cohort, channel_index)
            payload = f"warm:{cohort}:{channel_index:02}".encode()
            expected[channel] = payload
            send_command(publisher_sock, "PUBLISH", channel, payload)
            assert read_response(publisher_reader) >= 1

    for output_index, (sock, reader, pattern) in enumerate(subscribers):
        received = set()
        deadline = time.monotonic() + 10
        while len(received) < len(expected):
            remaining = deadline - time.monotonic()
            readable, _, _ = select.select([sock], [], [], max(0, remaining))
            assert readable, "Timed out warming output channels"
            message = read_response(reader)
            assert isinstance(message, list) and message[0] == b"pmessage", message
            source = message[2]
            parts = source.decode().split(":")
            cohort, channel_index = parts[3], parts[4]
            channel_index = int(channel_index)
            assert source == output_channel(output_index, cohort, channel_index), source
            assert message[3] == expected[source.replace(
                OUTPUT_PREFIXES[output_index] + b"mapped:", b"", 1
            ).rsplit(b":source:dest", 1)[0]], message
            received.add(source)
        assert len(received) == len(expected)


def percentile(sorted_values, percent):
    index = max(0, math.ceil(percent * len(sorted_values)) - 1)
    return sorted_values[index] / 1_000_000


def status_snapshot():
    with urllib.request.urlopen(STATUS_URL, timeout=5) as response:
        assert response.status == 200
        return json.loads(response.read())


def wait_for_drain(expected_input, received, timeout=60):
    deadline = time.monotonic() + timeout
    latest = None
    while time.monotonic() < deadline:
        latest = status_snapshot()
        with received.lock:
            counts = list(received.counts)
        outputs = latest.get("outputs", {})
        if (
            latest.get("input_messages_total") == expected_input
            and all(
                outputs.get(name, {}).get("input_messages_total") == expected_input
                and outputs[name].get("pending_messages") == 0
                and outputs[name].get("output_messages_total") == counts[index]
                for index, name in enumerate(OUTPUT_NAMES)
            )
        ):
                return latest
        time.sleep(0.05)
    raise AssertionError(f"Output workers did not drain mixed policies; last status={latest}")


class Received:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = [0, 0]
        self.cohorts = [{name: 0 for name in COHORTS} for _ in OUTPUT_NAMES]
        self.latencies_ns = [{name: [] for name in COHORTS} for _ in OUTPUT_NAMES]
        self.event_starts = {}
        self.failures = []
        self.stop = threading.Event()


def main():
    total_messages = PUBLISHERS * MESSAGES_PER_PUBLISHER
    warm_messages = len(COHORTS) * CHANNELS_PER_COHORT
    wait_for_input_subscription()
    subscribers = [open_output_subscriber(index) for index in range(2)]
    publisher_sock, publisher_reader = connect(0)
    try:
        warm_caches(publisher_sock, publisher_reader, subscribers)
        received = Received()
        received.counts[:] = [warm_messages, warm_messages]

        def consume_output(output_index, subscriber):
            sock, reader, pattern = subscriber
            try:
                while not received.stop.is_set():
                    readable, _, _ = select.select([sock], [], [], 0.1)
                    if not readable:
                        continue
                    message = read_response(reader)
                    assert isinstance(message, list) and message[0] == b"pmessage", message
                    assert message[1] == pattern, message
                    parts = message[2].decode().split(":")
                    assert len(parts) == 7 and parts[1:3] == ["mapped", "mix"], parts
                    cohort = parts[3]
                    assert cohort in COHORTS, cohort
                    payload = message[3]
                    with received.lock:
                        received.counts[output_index] += 1
                        received.cohorts[output_index][cohort] += 1
                        if cohort != "ttl":
                            started = received.event_starts.get(payload)
                            if started is not None:
                                received.latencies_ns[output_index][cohort].append(
                                    time.perf_counter_ns() - started
                                )
            except Exception as error:
                with received.lock:
                    received.failures.append((f"subscriber-{output_index}", repr(error)))
                received.stop.set()

        output_threads = [
            threading.Thread(target=consume_output, args=(index, subscriber), daemon=True)
            for index, subscriber in enumerate(subscribers)
        ]
        for thread in output_threads:
            thread.start()

        barrier = threading.Barrier(PUBLISHERS + 1)

        def publish_messages(publisher_id):
            sock, reader = connect(0)
            try:
                barrier.wait()
                for sequence in range(MESSAGES_PER_PUBLISHER):
                    event_index = publisher_id * MESSAGES_PER_PUBLISHER + sequence
                    cohort = COHORTS[event_index % len(COHORTS)]
                    channel_index = (event_index // len(COHORTS)) % CHANNELS_PER_COHORT
                    channel = source_channel(cohort, channel_index)
                    if cohort == "ttl":
                        payload = f"ttl:{channel_index:02}".encode()
                    else:
                        payload = f"{cohort}:{publisher_id}:{sequence}".encode()
                    with received.lock:
                        if cohort != "ttl":
                            received.event_starts[payload] = time.perf_counter_ns()
                    send_command(sock, "PUBLISH", channel, payload)
                    assert read_response(reader) >= 1
            except Exception as error:
                with received.lock:
                    received.failures.append((f"publisher-{publisher_id}", repr(error)))
                received.stop.set()
            finally:
                close(sock, reader)

        publisher_threads = [
            threading.Thread(target=publish_messages, args=(index,), daemon=True)
            for index in range(PUBLISHERS)
        ]
        for thread in publisher_threads:
            thread.start()

        wall_started = time.perf_counter()
        barrier.wait()
        for thread in publisher_threads:
            thread.join(timeout=60)
            assert not thread.is_alive(), "publisher thread timed out"
        publisher_phase = time.perf_counter()
        status = wait_for_drain(total_messages + warm_messages, received)
        wall_finished = time.perf_counter()
        received.stop.set()
        for thread in output_threads:
            thread.join(timeout=5)
        assert not received.failures, received.failures[:3]

        print(
            "hotpath-policy-mix "
            f"publishers={PUBLISHERS} messages_per_publisher={MESSAGES_PER_PUBLISHER} "
            f"total_input={total_messages} direct~33% conflate_200ms~33% "
            f"conflate_200ms_ttl5s~34% ttl_payload=repeated_per_channel "
            f"publish_phase_s={publisher_phase - wall_started:.3f} "
            f"output_drain_s={wall_finished - publisher_phase:.3f} "
            f"total_s={wall_finished - wall_started:.3f}"
        )
        for output_index, name in enumerate(OUTPUT_NAMES):
            metrics = status["outputs"][name]
            with received.lock:
                cohort_counts = dict(received.cohorts[output_index])
                cohort_latencies = {
                    cohort: sorted(received.latencies_ns[output_index][cohort])
                    for cohort in ("direct", "conflate")
                }
            direct = cohort_latencies["direct"]
            conflate = cohort_latencies["conflate"]
            print(
                f"output {name} delivered={received.counts[output_index] - warm_messages} "
                f"by_cohort={cohort_counts} "
                f"direct_p50_p95_ms={percentile(direct, 0.50):.3f}/{percentile(direct, 0.95):.3f} "
                f"conflate_p50_p95_ms={percentile(conflate, 0.50):.3f}/{percentile(conflate, 0.95):.3f} "
                f"conflated_total={metrics['conflated_messages_total']} "
                f"deduplicated_total={metrics['deduplicated_messages_total']}"
            )

        with urllib.request.urlopen(FILTERS_URL, timeout=10) as response:
            cache_snapshot = json.loads(response.read())["caches"]
        for name, cache in cache_snapshot.items():
            print(
                f"cache {name} capacity={cache['capacity']} entries={cache['entry_count']} "
                f"hits={cache['hits']} misses={cache['misses']} evictions={cache['evictions']}"
            )
    finally:
        close(publisher_sock, publisher_reader)
        for sock, reader, _ in subscribers:
            close(sock, reader)


if __name__ == "__main__":
    main()
