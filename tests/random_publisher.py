from redis_resp import connect, read_response, send_command
from payloads import DECOY_CHANNELS, make_publications


START_CHANNEL = "test-control:start"
DONE_CHANNEL = "test-control:done"


def publish_many(sock, reader, publications):
    for _, channel, payload in publications:
        send_command(sock, "PUBLISH", channel, payload)
    return [read_response(reader) for _ in publications]


def publish_test_messages(publisher_sock, publisher_reader):
    feed_publications, _, publications = make_publications()
    subscriber_counts = publish_many(publisher_sock, publisher_reader, publications)

    feed_counts = [
        count
        for (kind, _, _), count in zip(publications, subscriber_counts)
        if kind == "feed"
    ]
    decoy_counts = [
        count
        for (kind, _, _), count in zip(publications, subscriber_counts)
        if kind == "decoy"
    ]
    assert all(count >= 1 for count in feed_counts), feed_counts
    assert decoy_counts == [0] * len(DECOY_CHANNELS), decoy_counts

    send_command(publisher_sock, "PUBLISH", DONE_CHANNEL, b"done")
    assert read_response(publisher_reader) >= 1
    print(
        f"Published {len(feed_publications)} raw feed messages and "
        f"{len(DECOY_CHANNELS)} decoys on Redis DB0."
    )


def main():
    control_sock, control_reader = connect(0)
    send_command(control_sock, "SUBSCRIBE", START_CHANNEL)
    subscription_ack = read_response(control_reader)
    assert subscription_ack[0] == b"subscribe"
    control_sock.settimeout(None)

    publisher_sock, publisher_reader = connect(0)
    while True:
        start_message = read_response(control_reader)
        assert (
            start_message[0] == b"message"
            and start_message[1] == START_CHANNEL.encode()
            and start_message[2] == b"publish"
        ), start_message
        publish_test_messages(publisher_sock, publisher_reader)


if __name__ == "__main__":
    main()
