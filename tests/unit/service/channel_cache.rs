use super::ChannelLruCache;

#[test]
fn channel_cache_evicts_only_the_least_recently_used_entry() {
    let mut cache = ChannelLruCache::new(2);
    cache.insert("a", 1);
    cache.insert("b", 2);
    assert_eq!(cache.get("a"), Some(1));
    cache.insert("c", 3);

    assert_eq!(cache.get("a"), Some(1));
    assert_eq!(cache.get("b"), None);
    assert_eq!(cache.get("c"), Some(3));
    assert_eq!(cache.entries.len(), 2);
    assert_eq!(cache.evictions, 1);

    let snapshot = cache
        .snapshot_data()
        .into_json(|value| serde_json::json!(value));
    assert_eq!(snapshot["entries_most_recent_first"][0]["channel"], "c");
    assert_eq!(snapshot["entries_most_recent_first"][1]["channel"], "a");
}

#[test]
fn channel_cache_updates_values_and_handles_single_entry_capacity() {
    let mut cache = ChannelLruCache::new(1);
    cache.insert("a", 1);
    cache.insert("a", 2);
    assert_eq!(cache.get("a"), Some(2));
    cache.insert("b", 3);

    assert_eq!(cache.get("a"), None);
    assert_eq!(cache.get("b"), Some(3));
    assert_eq!(cache.evictions, 1);
}

#[test]
fn channel_cache_counts_hits_misses_and_churn_evictions() {
    let mut cache = ChannelLruCache::new(4);
    for index in 0..10 {
        let channel = format!("channel-{index}");
        assert_eq!(cache.get(&channel), None);
        cache.insert(&channel, index);
    }

    let snapshot = cache
        .snapshot_data()
        .into_json(|value| serde_json::json!(value));
    assert_eq!(snapshot["capacity"], 4);
    assert_eq!(snapshot["entry_count"], 4);
    assert_eq!(snapshot["hits"], 0);
    assert_eq!(snapshot["misses"], 10);
    assert_eq!(snapshot["evictions"], 6);
    assert_eq!(
        snapshot["entries_most_recent_first"][0]["channel"],
        "channel-9"
    );
    assert_eq!(
        snapshot["entries_most_recent_first"][3]["channel"],
        "channel-6"
    );

    assert_eq!(cache.get("channel-9"), Some(9));
    assert_eq!(
        cache
            .snapshot_data()
            .into_json(|value| serde_json::json!(value))["hits"],
        1
    );
}
