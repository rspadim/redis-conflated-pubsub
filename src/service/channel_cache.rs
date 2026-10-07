use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use serde_json::{Value, json};

use crate::status::CacheSnapshotter;

struct Node<V> {
    key: Arc<str>,
    value: V,
    previous: Option<usize>,
    next: Option<usize>,
}

pub(super) struct ChannelLruCache<V> {
    entries: HashMap<Arc<str>, usize>,
    nodes: Vec<Option<Node<V>>>,
    most_recent: Option<usize>,
    least_recent: Option<usize>,
    capacity: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
}

struct ChannelLruSnapshot<V> {
    capacity: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
    entries: Vec<(Arc<str>, V)>,
}

impl<V> ChannelLruSnapshot<V> {
    fn into_json(self, render_value: fn(&V) -> Value) -> Value {
        let entries = self
            .entries
            .into_iter()
            .map(|(channel, value)| {
                json!({
                    "channel": channel.as_ref(),
                    "value": render_value(&value),
                })
            })
            .collect::<Vec<_>>();
        json!({
            "capacity": self.capacity,
            "entry_count": entries.len(),
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "entries_most_recent_first": entries,
        })
    }
}

impl<V: Clone> ChannelLruCache<V> {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            nodes: Vec::new(),
            most_recent: None,
            least_recent: None,
            capacity,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    fn get(&mut self, channel: &str) -> Option<V> {
        let Some(index) = self.entries.get(channel).copied() else {
            self.misses = self.misses.saturating_add(1);
            return None;
        };
        self.hits = self.hits.saturating_add(1);
        self.mark_most_recent(index);
        self.nodes[index].as_ref().map(|node| node.value.clone())
    }

    fn insert(&mut self, channel: &str, value: V) {
        if self.capacity == 0 {
            return;
        }
        if let Some(index) = self.entries.get(channel).copied() {
            self.nodes[index].as_mut().unwrap().value = value;
            self.mark_most_recent(index);
            return;
        }

        let index = if self.entries.len() == self.capacity {
            self.evict_least_recent()
        } else {
            let index = self.nodes.len();
            self.nodes.push(None);
            index
        };
        let key: Arc<str> = Arc::from(channel);
        self.entries.insert(Arc::clone(&key), index);
        self.nodes[index] = Some(Node {
            key,
            value,
            previous: None,
            next: None,
        });
        self.mark_most_recent(index);
    }

    fn evict_least_recent(&mut self) -> usize {
        let index = self.least_recent.expect("a full cache has an LRU entry");
        self.unlink(index);
        let evicted = self.nodes[index].take().unwrap();
        self.entries.remove(evicted.key.as_ref());
        self.evictions = self.evictions.saturating_add(1);
        index
    }

    fn mark_most_recent(&mut self, index: usize) {
        if self.most_recent == Some(index) {
            return;
        }
        if self.nodes[index].as_ref().unwrap().previous.is_some()
            || self.nodes[index].as_ref().unwrap().next.is_some()
            || self.least_recent == Some(index)
        {
            self.unlink(index);
        }

        let previous_most_recent = self.most_recent;
        {
            let node = self.nodes[index].as_mut().unwrap();
            node.previous = None;
            node.next = previous_most_recent;
        }
        if let Some(previous_most_recent) = previous_most_recent {
            self.nodes[previous_most_recent].as_mut().unwrap().previous = Some(index);
        } else {
            self.least_recent = Some(index);
        }
        self.most_recent = Some(index);
    }

    fn unlink(&mut self, index: usize) {
        let node = self.nodes[index].as_ref().unwrap();
        let previous = node.previous;
        let next = node.next;
        if let Some(previous) = previous {
            self.nodes[previous].as_mut().unwrap().next = next;
        } else {
            self.most_recent = next;
        }
        if let Some(next) = next {
            self.nodes[next].as_mut().unwrap().previous = previous;
        } else {
            self.least_recent = previous;
        }
        let node = self.nodes[index].as_mut().unwrap();
        node.previous = None;
        node.next = None;
    }

    fn snapshot_data(&self) -> ChannelLruSnapshot<V> {
        let mut entries = Vec::with_capacity(self.entries.len());
        let mut current = self.most_recent;
        while let Some(index) = current {
            let node = self.nodes[index].as_ref().unwrap();
            entries.push((Arc::clone(&node.key), node.value.clone()));
            current = node.next;
        }
        ChannelLruSnapshot {
            capacity: self.capacity,
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            entries,
        }
    }
}

enum CacheStorage<V: Clone> {
    // Normal operation keeps each LRU local to its worker.
    Local(RefCell<ChannelLruCache<V>>),
    // Opt-in /filters inspection shares this one LRU with its snapshot callback.
    Shared(Arc<Mutex<ChannelLruCache<V>>>),
}

pub(super) struct ChannelCache<V: Clone> {
    storage: CacheStorage<V>,
    capacity: usize,
}

impl<V: Clone + Send + 'static> ChannelCache<V> {
    pub(super) fn new(capacity: usize, expose_to_http: bool) -> Self {
        let cache = ChannelLruCache::new(capacity);
        let storage = if expose_to_http {
            CacheStorage::Shared(Arc::new(Mutex::new(cache)))
        } else {
            CacheStorage::Local(RefCell::new(cache))
        };
        Self { storage, capacity }
    }

    pub(super) fn get(&self, channel: &str) -> Option<V> {
        match &self.storage {
            CacheStorage::Local(cache) => cache.borrow_mut().get(channel),
            CacheStorage::Shared(cache) => cache.lock().unwrap().get(channel),
        }
    }

    pub(super) fn insert(&self, channel: &str, value: V) {
        match &self.storage {
            CacheStorage::Local(cache) => cache.borrow_mut().insert(channel, value),
            CacheStorage::Shared(cache) => cache.lock().unwrap().insert(channel, value),
        }
    }

    pub(super) fn is_enabled(&self) -> bool {
        self.capacity > 0
    }

    #[cfg(test)]
    pub(super) fn entry_count(&self) -> usize {
        match &self.storage {
            CacheStorage::Local(cache) => cache.borrow().entries.len(),
            CacheStorage::Shared(cache) => cache.lock().unwrap().entries.len(),
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot_for_test(&self, render_value: fn(&V) -> Value) -> Value {
        match &self.storage {
            CacheStorage::Local(cache) => cache.borrow().snapshot_data().into_json(render_value),
            CacheStorage::Shared(cache) => {
                let snapshot = cache.lock().unwrap().snapshot_data();
                snapshot.into_json(render_value)
            }
        }
    }

    pub(super) fn inspector(&self, render_value: fn(&V) -> Value) -> Option<CacheSnapshotter> {
        let CacheStorage::Shared(cache) = &self.storage else {
            return None;
        };
        let cache = Arc::clone(cache);
        Some(Arc::new(move || {
            let snapshot = cache.lock().unwrap().snapshot_data();
            snapshot.into_json(render_value)
        }))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/service/channel_cache.rs"]
mod tests;
