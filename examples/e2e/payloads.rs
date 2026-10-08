//! Deterministic feed/decoy workload shared by `random-publisher` and
//! `integration`, replacing `tests/payloads.py`.
//!
//! The Python helper seeded `random.Random(20261005)`. This port keeps the
//! same channel sets, message counts and payload prefixes with a local
//! SplitMix64 generator, so both subcommands derive byte-identical messages
//! without adding a random-number dependency.

pub const FEED_CHANNELS: [&str; 3] = ["test-feed:alpha", "test-feed:beta", "test-feed:gamma"];
pub const FEED_MESSAGE_COUNT: usize = 48;
pub const DECOY_CHANNELS: [&str; 5] = [
    "noise-feed:alpha",
    "test-feeds:beta",
    "test-feedish:gamma",
    "other:test-feed:delta",
    "test-feed",
];
pub const RANDOM_SEED: u64 = 20261005;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationKind {
    Feed,
    Decoy,
}

#[derive(Clone, Debug)]
pub struct Publication {
    pub kind: PublicationKind,
    pub channel: String,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Workload {
    /// Every raw feed message, in publish order.
    pub feed: Vec<(String, Vec<u8>)>,
    /// Latest payload per feed channel, in first-seen order.
    pub latest: Vec<(String, Vec<u8>)>,
    /// Feed and decoy publications interleaved in publish order.
    pub publications: Vec<Publication>,
}

pub fn make_publications() -> Workload {
    let mut rng = Rng::new(RANDOM_SEED);
    let mut feed: Vec<(String, Vec<u8>)> = Vec::new();

    for (index, channel) in FEED_CHANNELS.iter().enumerate() {
        let payload = if index == 0 {
            b"\x00\xffinitial-alpha".to_vec()
        } else {
            format!("initial:{channel}").into_bytes()
        };
        feed.push(((*channel).to_owned(), payload));
    }

    for sequence in 0..FEED_MESSAGE_COUNT {
        let channel = FEED_CHANNELS[(rng.next_u64() % FEED_CHANNELS.len() as u64) as usize];
        let payload = if sequence % 3 == 0 {
            let mut payload = b"\x00\xff".to_vec();
            payload.extend_from_slice(&rng.bytes(12));
            payload
        } else {
            format!("message:{sequence}:{:016x}", rng.next_u64()).into_bytes()
        };
        feed.push((channel.to_owned(), payload));
    }

    let mut alpha = b"\x00\xfffinal-alpha\x00".to_vec();
    alpha.extend_from_slice(&rng.bytes(41));
    feed.push((FEED_CHANNELS[0].to_owned(), alpha));

    let mut beta = b"final-beta:".to_vec();
    beta.extend_from_slice(&rng.bytes(44));
    feed.push((FEED_CHANNELS[1].to_owned(), beta));

    let mut gamma = b"\x00\xfffinal-gamma\xfe".to_vec();
    gamma.extend_from_slice(&rng.bytes(41));
    feed.push((FEED_CHANNELS[2].to_owned(), gamma));

    let mut latest: Vec<(String, Vec<u8>)> = Vec::new();
    for (channel, payload) in &feed {
        match latest.iter_mut().find(|(seen, _)| seen == channel) {
            Some((_, value)) => *value = payload.clone(),
            None => latest.push((channel.clone(), payload.clone())),
        }
    }

    let mut publications = Vec::with_capacity(feed.len() + DECOY_CHANNELS.len());
    for (index, (channel, payload)) in feed.iter().enumerate() {
        publications.push(Publication {
            kind: PublicationKind::Feed,
            channel: channel.clone(),
            payload: payload.clone(),
        });
        if index < DECOY_CHANNELS.len() {
            let mut decoy = b"decoy:\x00\xff".to_vec();
            decoy.extend_from_slice(&rng.bytes(8));
            publications.push(Publication {
                kind: PublicationKind::Decoy,
                channel: DECOY_CHANNELS[index].to_owned(),
                payload: decoy,
            });
        }
    }

    Workload {
        feed,
        latest,
        publications,
    }
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn bytes(&mut self, length: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(length + 8);
        while bytes.len() < length {
            bytes.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        bytes.truncate(length);
        bytes
    }
}
