#![allow(dead_code)]
use std::{sync::Arc, time::Duration};

use ethers::prelude::Http;
use ethers::{
    abi::{AbiDecode, Address, RawLog},
    contract::EthEvent,
    prelude::EthLogDecode,
    providers::{Middleware, Provider, Ws},
    types::{BlockNumber, Filter, Log, Transaction, H256},
};
use futures::{Sink, SinkExt, StreamExt};

use client::{
    zksync_contract::{
        codegen::{
            BlockCommitFilter, BlockExecutionFilter, BlocksVerificationFilter, CommitBatchesCall,
        },
        parse_withdrawal_events_l1,
    },
    BlockEvent, ZksyncMiddleware,
};
use ethers_log_decode::EthLogDecode;

use crate::{metrics::CHAIN_EVENTS_METRICS, Error, Result};

// Total timecap for tx querying retry 10 minutes
const PENDING_TX_RETRY: usize = 12 * 10;
const PENDING_TX_RETRY_BACKOFF: Duration = Duration::from_secs(5);

#[derive(EthLogDecode)]
enum L1Events {
    BlockCommit(BlockCommitFilter),
    BlocksVerification(BlocksVerificationFilter),
    BlocksExecution(BlockExecutionFilter),
}

// A convenience multiplexer for `Block`-related events.
//
// The only purose of this structure is multpliexing together
// the `Block` events from the middleware as currently `ethers` events
// api relies on lifetimes and borrowing is hard to use otherwise
// in the async context.
/// Listener of block events on L1.
pub struct BlockEvents {
    ws_url: String,
    http_url: String,
}

impl BlockEvents {
    /// Creates a new `BlockEvents` structure
    ///
    /// # Arguments
    ///
    /// * `middleware`: The middleware to perform requests with.
    pub fn new(ws_url: &str, http_url: &str) -> BlockEvents {
        Self {
            ws_url: ws_url.to_string(),
            http_url: http_url.to_string(),
        }
    }

    async fn connect(&self) -> Option<Provider<Ws>> {
        match Provider::<Ws>::connect_with_reconnects(&self.ws_url, 0).await {
            Ok(p) => {
                CHAIN_EVENTS_METRICS.successful_l1_reconnects.inc();
                Some(p)
            }
            Err(e) => {
                tracing::warn!("Block events stream reconnect attempt failed: {e}");
                CHAIN_EVENTS_METRICS.l1_reconnects_on_error.inc();
                None
            }
        }
    }

    /// Run the main loop with re-connecting on websocket disconnects
    //
    // Websocket subscriptions do not work well with reconnections
    // in `ethers-rs`: https://github.com/gakonst/ethers-rs/issues/2418
    // This function is a workaround for that and implements manual re-connecting.
    pub async fn run_with_reconnects<B, S, M2>(
        self,
        diamond_proxy_addr: Address,
        l2_erc20_bridge_addr: Address,
        from_block: B,
        sender: S,
        l2_client: M2,
    ) -> Result<()>
    where
        B: Into<BlockNumber> + Copy,
        S: Sink<BlockEvent> + Unpin + Clone,
        <S as Sink<BlockEvent>>::Error: std::fmt::Debug,
        M2: ZksyncMiddleware + 'static,
    {
        let mut from_block: BlockNumber = from_block.into();
        let middleware = Arc::new(Provider::<Http>::try_from(self.http_url).unwrap());

        loop {
            match Self::run(
                diamond_proxy_addr,
                l2_erc20_bridge_addr,
                from_block,
                sender.clone(),
                middleware.clone(),
                &l2_client,
            )
            .await
            {
                Err(e) => {
                    tracing::warn!("Block events worker failed with {e}");
                }
                Ok(block) => from_block = block,
            }
        }
    }
}

impl BlockEvents {
    /// A convenience function that listens for all `Block`-related and sends them to the user.
    ///
    /// `ethers` APIs have two approaches to querying events from chain:
    ///   1. Listen to *all* types of events (will generate a less-performant code)
    ///   2. Listen to a *single* type of event
    ///
    /// The purpose of this function is two wrap the second approach
    /// and conveniently run a background loop for it sending all
    /// needed events to the user.
    ///
    /// This implementation is the only possible since `ethers` async
    /// APIs heavily rely on `&self` and what is worse on `&self`
    /// lifetimes making it practically impossible to decouple
    /// `Event` and `EventStream` types from each other.
    async fn run<B, S, M, M2>(
        diamond_proxy_addr: Address,
        l2_erc20_bridge_addr: Address,
        from_block: B,
        mut sender: S,
        middleware: M,
        l2_client: M2,
    ) -> Result<BlockNumber>
    where
        B: Into<BlockNumber> + Copy,
        M: Middleware,
        S: Sink<BlockEvent> + Unpin,
        <S as Sink<BlockEvent>>::Error: std::fmt::Debug,
        M2: ZksyncMiddleware,
    {
        let mut last_seen_block: BlockNumber = from_block.into();
        let latest_finalized_block = middleware
            .get_block(BlockNumber::Latest)
            .await
            .map_err(|e| Error::Middleware(e.to_string()))?
            .expect("last block number always exists in a live network; qed")
            .number
            .expect("last block always has a number; qed");

        if last_seen_block.as_number().unwrap() == latest_finalized_block {
            tracing::info!(
                "Block events has been synchronized to the latest L1[{latest_finalized_block}]."
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
            return Ok(last_seen_block);
        }

        tracing::info!(
            "Filtering logs from {} to {}",
            from_block
                .into()
                .as_number()
                .expect("always starting from a numbered block; qed")
                .as_u64(),
            latest_finalized_block.as_u64(),
        );

        let past_filter = Filter::new()
            .from_block(from_block)
            .to_block(latest_finalized_block)
            .address(diamond_proxy_addr)
            .topic0(vec![
                BlockCommitFilter::signature(),
                BlocksVerificationFilter::signature(),
                BlockExecutionFilter::signature(),
            ]);

        let mut logs = middleware.get_logs_paginated(&past_filter, 2000);

        while let Some(log) = logs.next().await {
            let Ok(log) = log else {
                tracing::warn!("L1 block events stream ended with {}", log.unwrap_err());
                return Ok(last_seen_block);
            };

            let Some(block_number) = log.block_number.map(|bn| bn.as_u64()) else {
                continue;
            };
            last_seen_block = block_number.into();

            let raw_log: RawLog = log.clone().into();
            if let Ok(l1_event) = L1Events::decode_log(&raw_log) {
                process_l1_event(
                    l2_erc20_bridge_addr,
                    &log,
                    &l1_event,
                    &middleware,
                    &l2_client,
                    &mut sender,
                )
                .await?;
            }
        }

        tracing::info!("all event streams have terminated, exiting...");
        last_seen_block = latest_finalized_block.as_u64().into();

        Ok(last_seen_block)
    }
}

async fn get_tx_with_retries<M>(
    middleware: &M,
    tx_hash: H256,
    retries: usize,
) -> Result<Option<Transaction>>
where
    M: Middleware,
{
    for _ in 0..retries {
        if let Ok(Some(tx)) = middleware.get_transaction(tx_hash).await {
            if tx.block_number.is_some() {
                return Ok(Some(tx));
            }
        }

        tokio::time::sleep(PENDING_TX_RETRY_BACKOFF).await;
    }

    Ok(None)
}

async fn process_l1_event<M, M2, S>(
    l2_erc20_bridge_addr: Address,
    log: &Log,
    l1_event: &L1Events,
    middleware: M,
    l2_client: M2,
    sender: &mut S,
) -> Result<()>
where
    M: Middleware,
    S: Sink<BlockEvent> + Unpin,
    <S as Sink<BlockEvent>>::Error: std::fmt::Debug,
    M2: ZksyncMiddleware,
{
    let Some(block_number) = log.block_number.map(|bn| bn.as_u64()) else {
        return Ok(());
    };

    match l1_event {
        L1Events::BlockCommit(bc) => {
            let Ok(tx) = get_tx_with_retries(
                &middleware,
                log.transaction_hash.unwrap_or_else(|| {
                    panic!("log always has a related transaction {:?}; qed", log)
                }),
                PENDING_TX_RETRY,
            )
            .await
            else {
                tracing::error!("Failed to retreive transaction {:?}", log.transaction_hash);
                return Err(Error::NoTransaction);
            };

            let tx = tx.unwrap_or_else(|| {
                panic!("mined transaction exists {:?}; qed", log.transaction_hash)
            });

            let mut events = vec![];

            if let Ok(commit_batches) = CommitBatchesCall::decode(&tx.input) {
                let mut pubdata = Vec::with_capacity(commit_batches.new_batches_data.len());
                for batch in commit_batches.new_batches_data.iter() {
                    pubdata.push(
                        l2_client
                            .get_batch_data_availability(batch.batch_number as u32)
                            .await?
                            .unwrap(),
                    );
                    tracing::info!(
                        "Get Batch[{}] data availability successfully.",
                        batch.batch_number
                    );
                }

                let mut res = parse_withdrawal_events_l1(
                    &commit_batches,
                    pubdata,
                    tx.block_number
                        .unwrap_or_else(|| {
                            panic!("a mined transaction {:?} has a block number; qed", tx.hash)
                        })
                        .as_u64(),
                    l2_erc20_bridge_addr,
                );
                events.append(&mut res);
            } else {
                panic!(
                    "Failed to decode CommitBatchesCall: {:?}; qed",
                    log.transaction_hash
                );
            }
            sender
                .send(BlockEvent::L2ToL1Events { events })
                .await
                .map_err(|_| Error::ChannelClosing)?;

            CHAIN_EVENTS_METRICS.block_commit_events.inc();
            sender
                .send(BlockEvent::BlockCommit {
                    block_number,
                    event: bc.clone(),
                })
                .await
                .map_err(|_| Error::ChannelClosing)?;
        }
        L1Events::BlocksVerification(event) => {
            CHAIN_EVENTS_METRICS.block_verification_events.inc();
            sender
                .send(BlockEvent::BlocksVerification {
                    block_number,
                    event: event.clone(),
                })
                .await
                .map_err(|_| Error::ChannelClosing)?;
        }
        L1Events::BlocksExecution(event) => {
            CHAIN_EVENTS_METRICS.block_execution_events.inc();
            sender
                .send(BlockEvent::BlockExecution {
                    block_number,
                    event: event.clone(),
                })
                .await
                .map_err(|_| Error::ChannelClosing)?;
        }
    }
    Ok(())
}
