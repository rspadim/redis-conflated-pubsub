use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::sync::{Notify, mpsc};
use tracing::warn;

use crate::config::QueueOverflowPolicy;

use super::QueuedInboundMessage;

/// Byte cost of one queued message: mapped channel plus payload.
pub(super) fn queued_message_bytes(message: &QueuedInboundMessage) -> usize {
    message.message.output_channel.len() + message.message.payload.len()
}

/// Logical queue limits shared by admission checks and the drop_oldest queue.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct QueueLimits {
    max_messages: Option<usize>,
    max_bytes: Option<usize>,
}

impl QueueLimits {
    pub(super) fn new(max_messages: Option<usize>, max_bytes: Option<usize>) -> Self {
        Self {
            max_messages,
            max_bytes,
        }
    }

    /// Admission check for `drop_newest`: the gauges already include every
    /// message accepted by the fan-out but not yet published, so this bounds
    /// both the channel backlog and the worker's in-flight window.
    pub(super) fn rejects_admission(
        self,
        pending_messages: u64,
        pending_queue_bytes: u64,
        incoming_queue_bytes: u64,
    ) -> bool {
        self.max_messages
            .is_some_and(|max| pending_messages >= max as u64)
            || self.max_bytes.is_some_and(|max| {
                pending_queue_bytes.saturating_add(incoming_queue_bytes) > max as u64
            })
    }

    fn exceeded(self, messages: usize, bytes: usize) -> bool {
        self.max_messages.is_some_and(|max| messages > max)
            || self.max_bytes.is_some_and(|max| bytes > max)
    }
}

/// Builds the producer/consumer pair for one output. Only `drop_oldest` needs
/// the shared FIFO, because an mpsc channel cannot pop from its front; every
/// other policy keeps the original unbounded mpsc behavior.
pub(super) fn output_queue(
    policy: QueueOverflowPolicy,
) -> (OutputQueueSender, OutputQueueReceiver) {
    match policy {
        QueueOverflowPolicy::DropOldest => {
            let queue = Arc::new(DropOldestQueue::new());
            (
                OutputQueueSender::DropOldest(Arc::clone(&queue)),
                OutputQueueReceiver::DropOldest(queue),
            )
        }
        QueueOverflowPolicy::DropNewest | QueueOverflowPolicy::DropByAge => {
            let (sender, receiver) = mpsc::unbounded_channel();
            (
                OutputQueueSender::Unbounded(sender),
                OutputQueueReceiver::Unbounded(receiver),
            )
        }
    }
}

pub(super) enum OutputQueueSender {
    Unbounded(mpsc::UnboundedSender<QueuedInboundMessage>),
    DropOldest(Arc<DropOldestQueue>),
}

impl OutputQueueSender {
    /// Sends one message. `drop_oldest` enforces the limits inside the shared
    /// queue and returns the evicted messages, oldest first, so the caller can
    /// record shed metrics; other policies always return an empty vector.
    pub(super) fn send(
        &self,
        message: QueuedInboundMessage,
        limits: QueueLimits,
    ) -> Result<Vec<QueuedInboundMessage>, ()> {
        match self {
            Self::Unbounded(sender) => sender.send(message).map(|()| Vec::new()).map_err(|_| ()),
            Self::DropOldest(queue) => Ok(queue.push(message, limits)),
        }
    }
}

impl Drop for OutputQueueSender {
    fn drop(&mut self) {
        if let Self::DropOldest(queue) = self {
            queue.close();
        }
    }
}

pub(super) enum OutputQueueReceiver {
    Unbounded(mpsc::UnboundedReceiver<QueuedInboundMessage>),
    DropOldest(Arc<DropOldestQueue>),
}

impl OutputQueueReceiver {
    pub(super) fn try_recv(&mut self) -> TryRecv {
        match self {
            Self::Unbounded(receiver) => match receiver.try_recv() {
                Ok(message) => TryRecv::Message(message),
                Err(mpsc::error::TryRecvError::Empty) => TryRecv::Empty,
                Err(mpsc::error::TryRecvError::Disconnected) => TryRecv::Disconnected,
            },
            Self::DropOldest(queue) => queue.try_recv(),
        }
    }

    pub(super) async fn recv(&mut self) -> Option<QueuedInboundMessage> {
        match self {
            Self::Unbounded(receiver) => receiver.recv().await,
            Self::DropOldest(queue) => queue.recv().await,
        }
    }
}

pub(super) enum TryRecv {
    Message(QueuedInboundMessage),
    Empty,
    Disconnected,
}

#[derive(Default)]
struct DropOldestState {
    queue: VecDeque<QueuedInboundMessage>,
    bytes: usize,
}

/// Shared FIFO used only by the `drop_oldest` policy. The producer locks,
/// pushes the new message, then evicts from the front while a limit is
/// exceeded; the worker pops from the front in FIFO order.
pub(super) struct DropOldestQueue {
    state: Mutex<DropOldestState>,
    notify: Notify,
    closed: AtomicBool,
}

impl DropOldestQueue {
    fn new() -> Self {
        Self {
            state: Mutex::new(DropOldestState::default()),
            notify: Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn push(
        &self,
        message: QueuedInboundMessage,
        limits: QueueLimits,
    ) -> Vec<QueuedInboundMessage> {
        let mut state = self.state.lock().unwrap();
        state.bytes = state.bytes.saturating_add(queued_message_bytes(&message));
        state.queue.push_back(message);
        let mut evicted = Vec::new();
        while limits.exceeded(state.queue.len(), state.bytes) {
            let Some(front) = state.queue.pop_front() else {
                break;
            };
            state.bytes = state.bytes.saturating_sub(queued_message_bytes(&front));
            evicted.push(front);
        }
        drop(state);
        self.notify.notify_one();
        evicted
    }

    fn try_recv(&self) -> TryRecv {
        let mut state = self.state.lock().unwrap();
        if let Some(message) = state.queue.pop_front() {
            state.bytes = state.bytes.saturating_sub(queued_message_bytes(&message));
            return TryRecv::Message(message);
        }
        if self.closed.load(Ordering::Acquire) {
            return TryRecv::Disconnected;
        }
        TryRecv::Empty
    }

    async fn recv(&self) -> Option<QueuedInboundMessage> {
        loop {
            // Register before checking so a concurrent close/notify cannot be lost.
            let notified = self.notify.notified();
            {
                let mut state = self.state.lock().unwrap();
                if let Some(message) = state.queue.pop_front() {
                    state.bytes = state.bytes.saturating_sub(queued_message_bytes(&message));
                    return Some(message);
                }
                if self.closed.load(Ordering::Acquire) {
                    return None;
                }
            }
            notified.await;
        }
    }
}

const QUEUE_SHED_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// Rate-limited shed log shared by the fan-out (drop_newest/drop_oldest) and
/// the intake loop (drop_by_age), following `OutputFailureLog`.
#[derive(Default)]
pub(super) struct QueueShedLog {
    last_report: Option<tokio::time::Instant>,
    shed_messages: u64,
    shed_payload_bytes: u64,
}

impl QueueShedLog {
    pub(super) fn report(&mut self, output: &str, messages: usize, payload_bytes: usize) {
        self.shed_messages = self.shed_messages.saturating_add(messages as u64);
        self.shed_payload_bytes = self.shed_payload_bytes.saturating_add(payload_bytes as u64);
        let now = tokio::time::Instant::now();
        if self
            .last_report
            .is_none_or(|last| now.duration_since(last) >= QUEUE_SHED_LOG_INTERVAL)
        {
            warn!(
                output,
                shed_messages = self.shed_messages,
                shed_payload_bytes = self.shed_payload_bytes,
                "output_queue_shed"
            );
            self.last_report = Some(now);
            self.shed_messages = 0;
            self.shed_payload_bytes = 0;
        }
    }

    pub(super) fn flush_suppressed(&mut self, output: &str) {
        if self.shed_messages > 0 {
            warn!(
                output,
                shed_messages = self.shed_messages,
                shed_payload_bytes = self.shed_payload_bytes,
                "output_queue_shed_suppressed"
            );
            self.shed_messages = 0;
            self.shed_payload_bytes = 0;
        }
    }
}
