use anyhow::{Context, Result, bail};
use regex_automata::meta::Regex;
use serde_json::{Value, json};

use crate::config::{ChannelFilterRule, DEFAULT_CHANNEL_CACHE_MAX_ENTRIES, FilterAction};
use crate::status::Metrics;

use super::channel_cache::ChannelCache;
use super::policy::glob::{GlobPattern, GlobWorkspace};

enum CompiledFilterSelector {
    Glob(GlobPattern),
    Regex(Regex),
    Literal(String),
}

struct CompiledFilterRule {
    selector: CompiledFilterSelector,
    action: FilterAction,
}

pub(super) struct ChannelFilterSet {
    rules: Vec<CompiledFilterRule>,
    default_filter: FilterAction,
    decisions: ChannelCache<bool>,
}

impl ChannelFilterSet {
    #[cfg(test)]
    pub(super) fn compile(
        rules: &[ChannelFilterRule],
        default_filter: FilterAction,
    ) -> Result<Self> {
        Self::compile_with_inspector(
            rules,
            default_filter,
            DEFAULT_CHANNEL_CACHE_MAX_ENTRIES,
            None,
        )
    }

    pub(super) fn compile_with_inspector(
        rules: &[ChannelFilterRule],
        default_filter: FilterAction,
        cache_capacity: usize,
        inspector: Option<(&Metrics, String)>,
    ) -> Result<Self> {
        let mut compiled_rules = Vec::with_capacity(rules.len());
        for (index, rule) in rules.iter().enumerate() {
            let selector = match (
                rule.glob.as_deref(),
                rule.regex.as_deref(),
                rule.raw.as_deref(),
                rule.single.as_deref(),
                rule.string.as_deref(),
            ) {
                (Some(pattern), None, None, None, None) => {
                    CompiledFilterSelector::Glob(GlobPattern::parse(pattern))
                }
                (None, Some(pattern), None, None, None) => CompiledFilterSelector::Regex(
                    Regex::new(pattern)
                        .with_context(|| format!("filters[{index}].regex is invalid"))?,
                ),
                (None, None, Some(value), None, None)
                | (None, None, None, Some(value), None)
                | (None, None, None, None, Some(value)) => {
                    CompiledFilterSelector::Literal(value.to_owned())
                }
                _ => bail!(
                    "filters[{index}] must specify exactly one of glob, regex, raw, single, or string"
                ),
            };
            compiled_rules.push(CompiledFilterRule {
                selector,
                action: rule.action,
            });
        }
        let decisions = ChannelCache::new(cache_capacity, inspector.is_some());
        if let Some((metrics, name)) = inspector {
            metrics.register_channel_cache_inspector(
                name,
                decisions.inspector(filter_decision_json).unwrap(),
            );
        }
        Ok(Self {
            rules: compiled_rules,
            default_filter,
            decisions,
        })
    }

    pub(super) fn allows(&mut self, channel: &str) -> bool {
        if self.rules.is_empty() {
            return self.default_filter == FilterAction::Accept;
        }
        if self.decisions.is_enabled()
            && let Some(allowed) = self.decisions.get(channel)
        {
            return allowed;
        }

        let mut workspace = GlobWorkspace::default();
        let mut last_action = None;
        for rule in &self.rules {
            let matches = match &rule.selector {
                CompiledFilterSelector::Glob(pattern) => pattern.matches(channel, &mut workspace),
                CompiledFilterSelector::Regex(pattern) => pattern.is_match(channel),
                CompiledFilterSelector::Literal(value) => channel == value,
            };
            if matches {
                last_action = Some(rule.action);
            }
        }
        let allowed = last_action
            .map(|action| action == FilterAction::Accept)
            .unwrap_or(self.default_filter == FilterAction::Accept);

        if self.decisions.is_enabled() {
            self.decisions.insert(channel, allowed);
        }
        allowed
    }

    pub(super) fn is_active(&self) -> bool {
        !self.rules.is_empty() || self.default_filter == FilterAction::Deny
    }

    #[cfg(test)]
    pub(super) fn cached_channels(&self) -> usize {
        self.decisions.entry_count()
    }

    #[cfg(test)]
    pub(super) fn cache_snapshot(&self) -> Value {
        self.decisions.snapshot_for_test(filter_decision_json)
    }
}

impl Default for ChannelFilterSet {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            default_filter: FilterAction::Accept,
            decisions: ChannelCache::new(DEFAULT_CHANNEL_CACHE_MAX_ENTRIES, false),
        }
    }
}

fn filter_decision_json(allowed: &bool) -> Value {
    json!(if *allowed { "accept" } else { "deny" })
}
