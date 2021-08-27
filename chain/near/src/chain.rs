use anyhow::Error;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::prelude::{LightEthereumBlock, LightEthereumBlockExt};
use graph::sf::endpoints::FirehoseNetworkEndpoints;
use graph::{
    blockchain::{
        block_stream::{
            BlockStreamEvent, BlockStreamMetrics, BlockWithTriggers, FirehoseError,
            FirehoseMapper as FirehoseMapperTrait, TriggersAdapter as TriggersAdapterTrait,
        },
        firehose_block_stream::FirehoseBlockStream,
        Block, BlockHash, BlockPtr, Blockchain, IngestorAdapter as IngestorAdapterTrait,
        IngestorError,
    },
    cheap_clone::CheapClone,
    components::store::DeploymentLocator,
    log::factory::{ComponentLoggerConfig, ElasticComponentLoggerConfig},
    prelude::{
        async_trait, o, BlockNumber, ChainStore, EthereumBlockWithCalls, Logger, LoggerFactory,
        MetricsRegistry, SubgraphStore,
    },
    sf::bstream,
};
use prost::Message;
use std::sync::Arc;

use crate::data_source::{DataSourceTemplate, UnresolvedDataSourceTemplate};
use crate::RuntimeAdapter;
use crate::{
    data_source::{DataSource, UnresolvedDataSource},
    sf::pb,
    TriggerFilter,
};
use graph::blockchain::block_stream::{BlockStream, Cursor};
use graph::components::store::CursorStore;

pub struct Chain {
    logger_factory: LoggerFactory,
    name: String,
    registry: Arc<dyn MetricsRegistry>,
    firehose_endpoints: Arc<FirehoseNetworkEndpoints>,
    chain_store: Arc<dyn ChainStore>,
    subgraph_store: Arc<dyn SubgraphStore>,
}

impl std::fmt::Debug for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "chain: near")
    }
}

impl Chain {
    pub fn new(
        logger_factory: LoggerFactory,
        name: String,
        registry: Arc<dyn MetricsRegistry>,
        chain_store: Arc<dyn ChainStore>,
        subgraph_store: Arc<dyn SubgraphStore>,
        firehose_endpoints: FirehoseNetworkEndpoints,
    ) -> Self {
        Chain {
            logger_factory,
            name,
            registry,
            firehose_endpoints: Arc::new(firehose_endpoints),
            chain_store,
            subgraph_store,
        }
    }
}

#[async_trait]
impl Blockchain for Chain {
    type Block = BlockFinality;

    type DataSource = DataSource;

    type UnresolvedDataSource = UnresolvedDataSource;

    type DataSourceTemplate = DataSourceTemplate;

    type UnresolvedDataSourceTemplate = UnresolvedDataSourceTemplate;

    type TriggersAdapter = TriggersAdapter;

    type TriggerData = crate::trigger::NearTrigger;

    type FirehoseMapper = FirehoseMapper;

    type MappingTrigger = crate::trigger::MappingTrigger;

    type TriggerFilter = crate::adapter::TriggerFilter;

    type NodeCapabilities = ();

    type IngestorAdapter = IngestorAdapter;

    type RuntimeAdapter = RuntimeAdapter;

    fn triggers_adapter(
        &self,
        loc: &DeploymentLocator,
        capabilities: &Self::NodeCapabilities,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<Self::TriggersAdapter>, Error> {
        let logger = self
            .logger_factory
            .subgraph_logger(&loc)
            .new(o!("component" => "TriggersAdapter"));

        let adapter = TriggersAdapter {
            logger,
            chain_store: self.chain_store.cheap_clone(),
            unified_api_version,
        };
        Ok(Arc::new(adapter))
    }

    async fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        filter: Arc<TriggerFilter>,
        metrics: Arc<BlockStreamMetrics>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error> {
        if start_blocks.len() != 0 && start_blocks.len() != 1 {
            return Err(anyhow::format_err!(
                "accepting start_blocks lenght of 0 or 1, got {}",
                start_blocks.len()
            ));
        }

        let firehose_endpoint = match self.firehose_endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available",)),
        };

        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "FirehoseBlockStream"));

        let firehose_mapper = self.firehose_mapper();

        let cursor_store: Arc<dyn CursorStore> = self.subgraph_store.cursor(&deployment)?;
        let firehose_cursor = cursor_store.get_cursor()?;

        Ok(Box::new(FirehoseBlockStream::new(
            firehose_endpoint,
            firehose_cursor,
            firehose_mapper,
            self.node_id.clone(),
            deployment.hash,
            filter,
            start_blocks,
            logger,
        )))
    }

    fn firehose_mapper(&self) -> Arc<Self::FirehoseMapper> {
        let logger = self.logger_factory.component_logger("FirehoseMapper", None);
        let mapper = FirehoseMapper { logger };
        Arc::new(mapper)
    }

    fn ingestor_adapter(&self) -> Arc<Self::IngestorAdapter> {
        let logger = self
            .logger_factory
            .component_logger(
                "BlockIngestor",
                Some(ComponentLoggerConfig {
                    elastic: Some(ElasticComponentLoggerConfig {
                        index: String::from("block-ingestor-logs"),
                    }),
                }),
            )
            // FIXME (NEAR): This had `o!("provider" => eth_adapter.provider().to_string())`, let's do the same with the actual Firehose provider
            .new(o!());

        let adapter = IngestorAdapter {
            logger,
            chain_store: self.chain_store.clone(),
        };
        Arc::new(adapter)
    }

    fn chain_store(&self) -> Arc<dyn ChainStore> {
        self.chain_store.clone()
    }

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError> {
        // FIXME (NEAR): Hmmm, what to do with this?
        Ok(BlockPtr {
            hash: BlockHash::from(vec![0xab]),
            number: 0,
        })
        // let eth_adapter = self
        //     .eth_adapters
        //     .cheapest()
        //     .with_context(|| format!("no adapter for chain {}", self.name))?
        //     .clone();s
        // eth_adapter
        //     .block_pointer_from_number(logger, number)
        //     .compat()
        //     .await
    }

    fn runtime_adapter(&self) -> Arc<Self::RuntimeAdapter> {
        Arc::new(RuntimeAdapter {})
    }
}

/// This is used in `NearAdapter::triggers_in_block`, called when re-processing a block for
/// newly created data sources. This allows the re-processing to be reorg safe without having to
/// always fetch the full block data.
#[derive(Clone, Debug)]
pub enum BlockFinality {
    /// If a block is final, we only need the header and the triggers.
    Final(Arc<LightEthereumBlock>),

    // If a block may still be reorged, we need to work with more local data.
    NonFinal(EthereumBlockWithCalls),
}

impl BlockFinality {
    pub(crate) fn light_block(&self) -> Arc<LightEthereumBlock> {
        match self {
            BlockFinality::Final(block) => block.cheap_clone(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.cheap_clone(),
        }
    }
}

impl<'a> From<&'a BlockFinality> for BlockPtr {
    fn from(block: &'a BlockFinality) -> BlockPtr {
        match block {
            BlockFinality::Final(b) => BlockPtr::from(&**b),
            BlockFinality::NonFinal(b) => BlockPtr::from(&b.ethereum_block),
        }
    }
}

impl Block for BlockFinality {
    fn ptr(&self) -> BlockPtr {
        match self {
            BlockFinality::Final(block) => block.block_ptr(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.block_ptr(),
        }
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        match self {
            BlockFinality::Final(block) => block.parent_ptr(),
            BlockFinality::NonFinal(block) => block.ethereum_block.block.parent_ptr(),
        }
    }
}

pub struct DummyDataSourceTemplate;

pub struct TriggersAdapter {
    logger: Logger,
    chain_store: Arc<dyn ChainStore>,
    unified_api_version: UnifiedMappingApiVersion,
}

#[async_trait]
impl TriggersAdapterTrait<Chain> for TriggersAdapter {
    async fn scan_triggers(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, Error> {
        // FIXME (NEAR): Scanning triggers makes little sense in Firehose approach, let's see
        Ok(vec![])
        // blocks_with_triggers(
        //     self.eth_adapter.clone(),
        //     self.logger.clone(),
        //     self.chain_store.clone(),
        //     self.ethrpc_metrics.clone(),
        //     self.stopwatch_metrics.clone(),
        //     from,
        //     to,
        //     filter,
        //     self.unified_api_version.clone(),
        // )
        // .await
    }

    async fn triggers_in_block(
        &self,
        logger: &Logger,
        block: BlockFinality,
        filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        // FIXME (NEAR): Fix once it compiles properly
        Ok(BlockWithTriggers {
            block: (),
            trigger_data: vec![],
        })
        // let block = get_calls(
        //     self.eth_adapter.as_ref(),
        //     logger.clone(),
        //     self.ethrpc_metrics.clone(),
        //     filter.requires_traces(),
        //     block,
        // )
        // .await?;

        // match &block {
        //     BlockFinality::Final(_) => {
        //         let block_number = block.number() as BlockNumber;
        //         let blocks = blocks_with_triggers(
        //             self.eth_adapter.clone(),
        //             logger.clone(),
        //             self.chain_store.clone(),
        //             self.ethrpc_metrics.clone(),
        //             self.stopwatch_metrics.clone(),
        //             block_number,
        //             block_number,
        //             filter,
        //             self.unified_api_version.clone(),
        //         )
        //         .await?;
        //         assert!(blocks.len() == 1);
        //         Ok(blocks.into_iter().next().unwrap())
        //     }
        //     BlockFinality::NonFinal(full_block) => {
        //         let mut triggers = Vec::new();
        //         triggers.append(&mut parse_log_triggers(
        //             &filter.log,
        //             &full_block.ethereum_block,
        //         ));
        //         triggers.append(&mut parse_call_triggers(&filter.call, &full_block)?);
        //         triggers.append(&mut parse_block_triggers(filter.block.clone(), &full_block));
        //         Ok(BlockWithTriggers::new(block, triggers))
        //     }
        // }
    }

    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error> {
        // FIXME (NEAR): Unusure about this, replace with Firehose?
        Ok(true)
        // self.eth_adapter
        //     .is_on_main_chain(&self.logger, ptr.clone())
        //     .await
    }

    fn ancestor_block(
        &self,
        ptr: BlockPtr,
        offset: BlockNumber,
    ) -> Result<Option<BlockFinality>, Error> {
        // FIXME (NEAR): Commented out for none
        Ok(None)
        // let block = self.chain_store.ancestor_block(ptr, offset)?;
        // Ok(block.map(|block| {
        //     BlockFinality::NonFinal(EthereumBlockWithCalls {
        //         ethereum_block: block,
        //         calls: None,
        //     })
        // }))
    }

    /// Panics if `block` is genesis.
    /// But that's ok since this is only called when reverting `block`.
    async fn parent_ptr(&self, block: &BlockPtr) -> Result<BlockPtr, Error> {
        // FIXME (NEAR): I doubt we need this now, let's see
        Ok(BlockPtr {
            hash: BlockHash::from(vec![0xab]),
            number: 0,
        })
        // use futures::stream::Stream;
        // use graph::prelude::LightEthereumBlockExt;

        // let blocks = self
        //     .eth_adapter
        //     .load_blocks(
        //         self.logger.cheap_clone(),
        //         self.chain_store.cheap_clone(),
        //         HashSet::from_iter(Some(block.hash_as_h256())),
        //     )
        //     .collect()
        //     .compat()
        //     .await?;
        // assert_eq!(blocks.len(), 1);

        // // Expect: This is only called when reverting and therefore never for genesis.
        // Ok(blocks[0]
        //     .parent_ptr()
        //     .expect("genesis block cannot be reverted"))
    }
}

pub struct FirehoseMapper {
    logger: Logger,
}

impl FirehoseMapper {
    fn triggers_in_block(
        &self,
        _logger: &Logger,
        block: &pb::Block,
        filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, FirehoseError> {
        // FIXME (NEAR): Firehose related!
        Ok(BlockWithTriggers {
            block: (),
            trigger_data: (),
        })
        // let mut triggers = Vec::new();

        // let block_with_calls: EthereumBlockWithCalls = block.into();

        // triggers.append(&mut parse_log_triggers(
        //     &filter.log,
        //     &block_with_calls.ethereum_block,
        // ));
        // triggers.append(&mut parse_call_triggers(&filter.call, &block_with_calls)?);
        // triggers.append(&mut parse_block_triggers(
        //     filter.block.clone(),
        //     &block_with_calls,
        // ));

        // Ok(BlockWithTriggers::new(block.into(), triggers))
    }
}

impl FirehoseMapperTrait<Chain> for FirehoseMapper {
    fn to_block_stream_event(
        &self,
        response: &bstream::BlockResponseV2,
        filter: &TriggerFilter,
    ) -> Result<BlockStreamEvent<Chain>, FirehoseError> {
        let step = bstream::ForkStep::from_i32(response.step).unwrap_or_else(|| {
            panic!(
                "unknown step i32 value {}, maybe you forgot update & re-regenerate the protobuf definitions?",
                response.step
            )
        });
        let any_block = response
            .block
            .as_ref()
            .expect("block payload information should always be present");

        // Right now, this is done in all cases but in reality, with how the BlockStreamEvent::Revert
        // is defined right now, only block hash and block number is necessary. However, this information
        // is not part of the actual bstream::BlockResponseV2 payload. As such, we need to decode the full
        // block which is useless.
        //
        // Check about adding basic information about the block in the bstream::BlockResponseV2 or maybe
        // define a slimmed down stuct that would decode only a few fields and ignore all the rest.
        let block = pb::Block::decode(any_block.value.as_ref())?;

        match step {
            bstream::ForkStep::StepNew => Ok(BlockStreamEvent::ProcessBlock(
                self.triggers_in_block(&self.logger, &block, filter)?,
                Cursor::Some(response.cursor.clone()),
            )),

            bstream::ForkStep::StepUndo => Ok(BlockStreamEvent::Revert(
                BlockPtr {
                    hash: BlockHash::from(block.hash),
                    number: block.number as i32,
                },
                Cursor::Some(response.cursor.clone()),
            )),

            bstream::ForkStep::StepIrreversible => {
                panic!("irreversible step is not handled and should not be requested in the Firehose request")
            }

            bstream::ForkStep::StepUnknown => {
                panic!("unknown step should not happen in the Firehose response")
            }
        }
    }
}

pub struct IngestorAdapter {
    logger: Logger,
    chain_store: Arc<dyn ChainStore>,
}

#[async_trait]
impl IngestorAdapterTrait<Chain> for IngestorAdapter {
    fn logger(&self) -> &Logger {
        &self.logger
    }

    fn ancestor_count(&self) -> BlockNumber {
        0
    }

    async fn latest_block(&self) -> Result<BlockPtr, IngestorError> {
        Ok(BlockPtr {
            hash: BlockHash::from(vec![0xab]),
            number: 0,
        })
    }

    async fn ingest_block(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockHash>, IngestorError> {
        // FIXME (NEAR): Commented out for none
        Ok(None)
        // // TODO: H256::from_slice can panic
        // let block_hash = H256::from_slice(block_hash.as_slice());

        // // Get the fully populated block
        // let block = self
        //     .eth_adapter
        //     .block_by_hash(&self.logger, block_hash)
        //     .compat()
        //     .await?
        //     .ok_or_else(|| IngestorError::BlockUnavailable(block_hash))?;
        // let block = self
        //     .eth_adapter
        //     .load_full_block(&self.logger, block)
        //     .compat()
        //     .await?;

        // // Store it in the database and try to advance the chain head pointer
        // self.chain_store.upsert_block(block).await?;

        // self.chain_store
        //     .cheap_clone()
        //     .attempt_chain_head_update(self.ancestor_count)
        //     .await
        //     .map(|missing| missing.map(|h256| h256.into()))
        //     .map_err(|e| {
        //         error!(self.logger, "failed to update chain head");
        //         IngestorError::Unknown(e)
        //     })
    }

    fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error> {
        // FIXME (NEAR): Commented out for none
        Ok(None)
        // self.chain_store.chain_head_ptr()
    }

    fn cleanup_cached_blocks(&self) -> Result<Option<(i32, usize)>, Error> {
        // FIXME (NEAR): Commented out for none
        Ok(None)
        // self.chain_store.cleanup_cached_blocks(self.ancestor_count)
    }
}
