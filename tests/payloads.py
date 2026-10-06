import random


FEED_CHANNELS = (
    "test-feed:alpha",
    "test-feed:beta",
    "test-feed:gamma",
)
FEED_MESSAGE_COUNT = 48
DECOY_CHANNELS = (
    "noise-feed:alpha",
    "test-feeds:beta",
    "test-feedish:gamma",
    "other:test-feed:delta",
    "test-feed",
)
RANDOM_SEED = 20261005


def make_publications():
    randomizer = random.Random(RANDOM_SEED)
    feed_publications = []
    for index, channel in enumerate(FEED_CHANNELS):
        if index == 0:
            payload = b"\x00\xffinitial-alpha"
        else:
            payload = f"initial:{channel}".encode()
        feed_publications.append((channel, payload))

    for sequence in range(FEED_MESSAGE_COUNT):
        channel = randomizer.choice(FEED_CHANNELS)
        if sequence % 3 == 0:
            payload = b"\x00\xff" + randomizer.randbytes(12)
        else:
            payload = f"message:{sequence}:{randomizer.getrandbits(64):016x}".encode()
        feed_publications.append((channel, payload))

    feed_publications.extend(
        (
            (
                FEED_CHANNELS[0],
                b"\x00\xfffinal-alpha\x00" + randomizer.randbytes(41),
            ),
            (
                FEED_CHANNELS[1],
                b"final-beta:" + randomizer.randbytes(44),
            ),
            (
                FEED_CHANNELS[2],
                b"\x00\xfffinal-gamma\xfe" + randomizer.randbytes(41),
            ),
        )
    )

    latest_payloads = {}
    for channel, payload in feed_publications:
        latest_payloads[channel] = payload

    publications = []
    for index, (channel, payload) in enumerate(feed_publications):
        publications.append(("feed", channel, payload))
        if index < len(DECOY_CHANNELS):
            decoy_channel = DECOY_CHANNELS[index]
            decoy_payload = b"decoy:\x00\xff" + randomizer.randbytes(8)
            publications.append(("decoy", decoy_channel, decoy_payload))

    return feed_publications, latest_payloads, publications
