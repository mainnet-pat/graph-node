use graph::{
    blockchain::ChainHeadUpdateStream,
    parking_lot::RwLock,
    prelude::{
        futures03::{self, FutureExt},
        tokio, StoreError,
    },
    prometheus::GaugeVec,
};
use std::str::FromStr;
use std::sync::Arc;
use std::{collections::BTreeMap, time::Duration};

use diesel::RunQueryDsl;
use lazy_static::lazy_static;

use crate::{
    connection_pool::ConnectionPool,
    notification_listener::{JsonNotification, NotificationListener, SafeChannelName},
};
use graph::blockchain::ChainHeadUpdateListener as ChainHeadUpdateListenerTrait;
use graph::prelude::serde::{Deserialize, Serialize};
use graph::prelude::serde_json::{self, json};
use graph::prelude::tokio::sync::{mpsc::Receiver, watch};
use graph::prelude::{crit, debug, o, CheapClone, Logger, MetricsRegistry};

lazy_static! {
    pub static ref CHAIN_HEAD_WATCHER_TIMEOUT: Duration = Duration::from_secs(
        std::env::var("GRAPH_CHAIN_HEAD_WATCHER_TIMEOUT")
            .ok()
            .map(|s| u64::from_str(&s).unwrap_or_else(|_| panic!(
                "failed to parse env var GRAPH_CHAIN_HEAD_WATCHER_TIMEOUT"
            )))
            .unwrap_or(30)
    );
    pub static ref CHANNEL_NAME: SafeChannelName =
        SafeChannelName::i_promise_this_is_safe("chain_head_updates");
}

struct Watcher {
    sender: watch::Sender<()>,
    receiver: watch::Receiver<()>,
}

impl Watcher {
    fn new() -> Self {
        let (sender, receiver) = watch::channel(());
        Watcher { sender, receiver }
    }

    fn send(&self) {
        // Unwrap: `self` holds a receiver.
        self.sender.send(()).unwrap()
    }
}

pub struct BlockIngestorMetrics {
    chain_head_number: Box<GaugeVec>,
}

impl BlockIngestorMetrics {
    pub fn new(registry: Arc<dyn MetricsRegistry>) -> Self {
        Self {
            chain_head_number: registry
                .new_gauge_vec(
                    "ethereum_chain_head_number",
                    "Block number of the most recent block synced from Ethereum",
                    vec![String::from("network")],
                )
                .unwrap(),
        }
    }

    pub fn set_chain_head_number(&self, network_name: &str, chain_head_number: i64) {
        self.chain_head_number
            .with_label_values(vec![network_name].as_slice())
            .set(chain_head_number as f64);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChainHeadUpdate {
    pub network_name: String,
    pub head_block_hash: String,
    pub head_block_number: u64,
}

pub struct ChainHeadUpdateListener {
    /// Update watchers keyed by network.
    watchers: Arc<RwLock<BTreeMap<String, Watcher>>>,
    _listener: NotificationListener,
}

/// Sender for messages that the `ChainHeadUpdateListener` on other nodes
/// will receive. The sender is specific to a particular chain.
pub(crate) struct ChainHeadUpdateSender {
    pool: ConnectionPool,
    chain_name: String,
}

impl ChainHeadUpdateListener {
    pub fn new(logger: &Logger, registry: Arc<dyn MetricsRegistry>, postgres_url: String) -> Self {
        let logger = logger.new(o!("component" => "ChainHeadUpdateListener"));
        let ingestor_metrics = Arc::new(BlockIngestorMetrics::new(registry.clone()));

        // Create a Postgres notification listener for chain head updates
        let (mut listener, receiver) =
            NotificationListener::new(&logger, postgres_url, CHANNEL_NAME.clone());
        let watchers = Arc::new(RwLock::new(BTreeMap::new()));

        Self::listen(
            logger,
            ingestor_metrics,
            &mut listener,
            receiver,
            watchers.cheap_clone(),
        );

        ChainHeadUpdateListener {
            watchers,

            // We keep the listener around to tie its stream's lifetime to
            // that of the chain head update listener and prevent it from
            // terminating early
            _listener: listener,
        }
    }

    fn listen(
        logger: Logger,
        metrics: Arc<BlockIngestorMetrics>,
        listener: &mut NotificationListener,
        mut receiver: Receiver<JsonNotification>,
        watchers: Arc<RwLock<BTreeMap<String, Watcher>>>,
    ) {
        // Process chain head updates in a dedicated task
        graph::spawn(async move {
            while let Some(notification) = receiver.recv().await {
                // Create ChainHeadUpdate from JSON
                let update: ChainHeadUpdate =
                    match serde_json::from_value(notification.payload.clone()) {
                        Ok(update) => update,
                        Err(e) => {
                            crit!(
                                logger,
                                "invalid chain head update received from database";
                                "payload" => format!("{:?}", notification.payload),
                                "error" => e.to_string()
                            );
                            continue;
                        }
                    };

                // Observe the latest chain head for each network to monitor block ingestion
                metrics
                    .set_chain_head_number(&update.network_name, *&update.head_block_number as i64);

                // If there are subscriptions for this network, notify them.
                if let Some(watcher) = watchers.read().get(&update.network_name) {
                    watcher.send()
                }
            }
        });

        // We're ready, start listening to chain head updates
        listener.start();
    }
}

impl ChainHeadUpdateListenerTrait for ChainHeadUpdateListener {
    fn subscribe(&self, network_name: String, logger: Logger) -> ChainHeadUpdateStream {
        let update_receiver = {
            let existing = {
                let watchers = self.watchers.read();
                watchers.get(&network_name).map(|w| w.receiver.clone())
            };

            if let Some(watcher) = existing {
                // Common case, this is not the first subscription for this network.
                watcher
            } else {
                // This is the first subscription for this network, a lock is required.
                //
                // Race condition: Another task could have simoultaneously entered this branch and
                // inserted a writer, so we should check the entry again after acquiring the lock.
                self.watchers
                    .write()
                    .entry(network_name)
                    .or_insert_with(|| Watcher::new())
                    .receiver
                    .clone()
            }
        };

        Box::new(futures03::stream::unfold(
            update_receiver,
            move |mut update_receiver| {
                let logger = logger.clone();
                async move {
                    // To be robust against any problems with the listener for the DB channel, a
                    // timeout is set so that subscribers are guaranteed to get periodic updates.
                    match tokio::time::timeout(
                        *CHAIN_HEAD_WATCHER_TIMEOUT,
                        update_receiver.changed(),
                    )
                    .await
                    {
                        // Received an update.
                        Ok(Ok(())) => (),

                        // The sender was dropped, this should never happen.
                        Ok(Err(_)) => crit!(logger, "chain head watcher terminated"),

                        Err(_) => debug!(
                            logger,
                            "no chain head update for {} seconds, polling for update",
                            CHAIN_HEAD_WATCHER_TIMEOUT.as_secs()
                        ),
                    };
                    Some(((), update_receiver))
                }
                .boxed()
            },
        ))
    }
}

impl ChainHeadUpdateSender {
    pub fn new(pool: ConnectionPool, network_name: String) -> Self {
        Self {
            pool,
            chain_name: network_name,
        }
    }

    pub fn send(&self, hash: &str, number: i64) -> Result<(), StoreError> {
        use crate::functions::pg_notify;

        let msg = json! ({
            "network_name": &self.chain_name,
            "head_block_hash": hash,
            "head_block_number": number
        });

        let conn = self.pool.get()?;
        diesel::select(pg_notify(CHANNEL_NAME.as_str(), &msg.to_string()))
            .execute(&conn)
            .map_err(StoreError::from)
            .map(|_| ())
    }
}
