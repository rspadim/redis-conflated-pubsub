import base64
import json
import random
import time

from redis_resp import close, connect, read_response, send_command


READY_CHANNEL = "test-control:ready"
DONE_CHANNEL = "test-control:done"
FEED_CHANNELS = (
    "test-feed:alpha",
    "test-feed:beta",
    "test-feed:gamma",
)
FEED_MESSAGE_COUNT = 36
DECOY_CHANNELS = (
    "noise-feed:alpha",
    "test-feeds:beta",
    "test-feedish:gamma",
    "other:test-feed:delta",
    "test-feed",
)
RANDOM_SEED = 20261005


def publish_many(sock, reader, publications):
    for channel, payload in publications:
        send_command(sock, "PUBLISH", channel, payload)
    return [read_response(reader) for _ in publications]


def encode_control_payload(payload):
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return {"encoding": "base64", "data": base64.b64encode(payload).decode("ascii")}
    return {"encoding": "utf-8", "data": text}


def main():
    control_sock, control_reader = connect(2)
    send_command(control_sock, "SUBSCRIBE", READY_CHANNEL)
    subscription_ack = read_response(control_reader)
    assert subscription_ack[0] == b"subscribe"
    control_sock.settimeout(None)
    ready_message = read_response(control_reader)
    assert ready_message[0] == b"message" and ready_message[1] == READY_CHANNEL.encode()
    close(control_sock, control_reader)

    publisher_sock, publisher_reader = connect(2)
    randomizer = random.Random(RANDOM_SEED)
    latest_payloads = {}
    feed_publications = []

    for channel in FEED_CHANNELS:
        payload = f"initial:{randomizer.getrandbits(32):08x}".encode()
        feed_publications.append((channel, payload))
        latest_payloads[channel] = payload

    for sequence in range(FEED_MESSAGE_COUNT):
        channel = randomizer.choice(FEED_CHANNELS)
        if sequence % 2 == 0:
            payload = b"\x00\xff" + randomizer.randbytes(8)
        else:
            payload = f"{sequence}:{randomizer.getrandbits(64):016x}".encode()
        feed_publications.append((channel, payload))
        latest_payloads[channel] = payload

    for channel, payload in (
        (FEED_CHANNELS[0], b"\x00\xff" + randomizer.randbytes(16)),
        (FEED_CHANNELS[1], f"final:{randomizer.getrandbits(64):016x}".encode()),
        (FEED_CHANNELS[2], f"final:{randomizer.getrandbits(64):016x}".encode()),
    ):
        feed_publications.append((channel, payload))
        latest_payloads[channel] = payload

    feed_subscriber_counts = publish_many(
        publisher_sock, publisher_reader, feed_publications
    )
    assert all(count >= 1 for count in feed_subscriber_counts), feed_subscriber_counts

    decoy_publications = [
        (channel, f"ignored:{randomizer.getrandbits(32):08x}".encode())
        for channel in DECOY_CHANNELS
    ]
    decoy_subscriber_counts = publish_many(
        publisher_sock, publisher_reader, decoy_publications
    )
    assert decoy_subscriber_counts == [0] * len(DECOY_CHANNELS), decoy_subscriber_counts

    published_feed_count = len(feed_publications)

    completion = {
        "published_feed_count": published_feed_count,
        "latest_payloads": {
            channel: encode_control_payload(payload)
            for channel, payload in latest_payloads.items()
        },
    }
    send_command(
        publisher_sock,
        "PUBLISH",
        DONE_CHANNEL,
        json.dumps(completion, separators=(",", ":")),
    )
    assert read_response(publisher_reader) >= 1
    close(publisher_sock, publisher_reader)
    print(
        f"Published {published_feed_count} randomized feed messages and "
        f"{len(DECOY_CHANNELS)} decoys from database 2."
    )

    # Keep the Compose service alive until the integration-test container exits.
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
