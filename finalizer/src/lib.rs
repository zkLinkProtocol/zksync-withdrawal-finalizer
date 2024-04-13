#![deny(unused_crate_dependencies)]
#![warn(unused_extern_crates)]
#![warn(unused_imports)]

//! Finalization logic implementation.

use std::{collections::HashSet, str::FromStr, time::Duration};
use std::ops::Deref;

use accumulator::WithdrawalsAccumulator;
use ethers::{
    abi::Address,
    providers::{Middleware, MiddlewareError},
    types::{H256, U256},
};
use ethers::contract::ContractError;
use futures::TryFutureExt;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use client::{
    is_eth, withdrawal_finalizer::codegen::withdrawal_finalizer::Result as FinalizeResult,
    WithdrawalKey,
};
use client::{
    l1bridge::codegen::IL1Bridge, withdrawal_finalizer::codegen::WithdrawalFinalizer,
    zksync_contract::codegen::IZkSync, WithdrawalParams, ZksyncMiddleware,
};
use url::Url;
use withdrawals_meterer::{MeteringComponent, WithdrawalsMeter};

use crate::{
    error::{Error, Result},
    metrics::FINALIZER_METRICS,
};
pub use crate::second_chain::SecondChainFinalizer;

mod accumulator;
mod second_chain;
mod error;
mod metrics;

/// A limit to cap a transaction fee (in ether) for safety reasons.
const TX_FEE_LIMIT: f64 = 0.8;

/// When finalizer runs out of money back off this amount of time.
const OUT_OF_FUNDS_BACKOFF: Duration = Duration::from_secs(10);

/// Backoff period if one of the loop iterations has failed.
const LOOP_ITERATION_ERROR_BACKOFF: Duration = Duration::from_secs(5);

/// An `enum` that defines target that Finalizer finalizes.
#[allow(missing_docs)]
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum FinalizeWithdrawChain {
    All,
    PrimaryChain,
    SecondaryChain,
}

impl FromStr for FinalizeWithdrawChain {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

impl Default for FinalizeWithdrawChain {
    fn default() -> Self {
        Self::All
    }
}

/// An `enum` that defines a set of tokens that Finalizer finalizes.
#[derive(Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum TokenList {
    /// Finalize all known tokens
    All,
    /// Finalize nothing
    None,
    /// Finalize everything but these tokens, this is a blacklist.
    BlackList(Vec<Address>),
    /// Finalize nothing but these tokens, this is a whitelist.
    WhiteList(Vec<Address>),
}

impl Default for TokenList {
    fn default() -> Self {
        Self::All
    }
}

impl FromStr for TokenList {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let res = serde_json::from_str(s)?;
        Ok(res)
    }
}

/// A newtype that represents a set of addresses in JSON format.
#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct AddrList(pub Vec<Address>);

impl Deref for AddrList {
    type Target = [Address];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for AddrList {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let res: Vec<Address> = serde_json::from_str(s)?;
        Ok(AddrList(res))
    }
}

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct UrlList(pub Vec<Url>);

impl Deref for UrlList {
    type Target = [Url];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for UrlList {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let res: Vec<Url> = serde_json::from_str(s)?;
        Ok(UrlList(res))
    }
}

/// Finalizer.
#[derive(Clone)]
pub struct Finalizer<M1, M2> where
    M1: Clone,
    M2: Clone,
{
    pgpool: PgPool,
    one_withdrawal_gas_limit: U256,
    batch_finalization_gas_limit: U256,
    finalizer_contract: WithdrawalFinalizer<M1>,
    zksync_contract: IZkSync<M2>,
    l1_bridge: IL1Bridge<M2>,
    unsuccessful: Vec<WithdrawalParams>,

    no_new_withdrawals_backoff: Duration,
    query_db_pagination_limit: u64,
    tx_fee_limit: U256,
    tx_retry_timeout: Duration,
    account_address: Address,
    withdrawals_meterer: Option<WithdrawalsMeter>,
    token_list: TokenList,

    // None is primary chain, Some is secondary chain
    selected_gate_way_address: Option<Address>,
    second_chains: Vec<SecondChainFinalizer<M1, M2>>,

    eth_threshold: Option<U256>,
    finalize_withdraw_target: FinalizeWithdrawChain
}

const NO_NEW_WITHDRAWALS_BACKOFF: Duration = Duration::from_secs(5);
const QUERY_DB_PAGINATION_LIMIT: u64 = 50;

impl<S, M> Finalizer<S, M>
where
    S: Middleware + Clone + 'static,
    M: Middleware + Clone + 'static,
{
    /// Create a new [`Finalizer`].
    ///
    /// * `S` is expected to be a [`Middleware`] instance equipped with [`SignerMiddleware`]
    /// * `M` is expected to be an ordinary read-only middleware to read information from L1.
    ///
    /// [`SignerMiddleware`]: https://docs.rs/ethers/latest/ethers/middleware/struct.SignerMiddleware.html
    /// [`Middleware`]: https://docs.rs/ethers/latest/ethers/providers/trait.Middleware.html
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pgpool: PgPool,
        one_withdrawal_gas_limit: U256,
        batch_finalization_gas_limit: U256,
        finalizer_contract: WithdrawalFinalizer<S>,
        zksync_contract: IZkSync<M>,
        second_chains: Vec<SecondChainFinalizer<S, M>>,
        l1_bridge: IL1Bridge<M>,
        tx_retry_timeout: usize,
        account_address: Address,
        token_list: TokenList,
        meter_withdrawals: bool,
        eth_threshold: Option<U256>,
        finalize_withdraw_target: FinalizeWithdrawChain,
    ) -> Self {
        let withdrawals_meterer = meter_withdrawals.then_some(WithdrawalsMeter::new(
            pgpool.clone(),
            MeteringComponent::FinalizedWithdrawals,
        ));
        let tx_fee_limit = ethers::utils::parse_ether(TX_FEE_LIMIT)
            .expect("{TX_FEE_LIMIT} ether is a parsable amount; qed");

        tracing::info!("finalizing tokens {token_list:?}");

        Self {
            pgpool,
            one_withdrawal_gas_limit,
            batch_finalization_gas_limit,
            finalizer_contract,
            zksync_contract,
            l1_bridge,
            unsuccessful: vec![],
            no_new_withdrawals_backoff: NO_NEW_WITHDRAWALS_BACKOFF,
            query_db_pagination_limit: QUERY_DB_PAGINATION_LIMIT,
            tx_fee_limit,
            tx_retry_timeout: Duration::from_secs(tx_retry_timeout as u64),
            account_address,
            withdrawals_meterer,
            token_list,
            selected_gate_way_address: None,
            second_chains,
            eth_threshold,
            finalize_withdraw_target,
        }
    }

    pub fn finalizer_contract(&self) -> &WithdrawalFinalizer<S> {
        if let Some(address) = self.selected_gate_way_address {
            self.second_chains
                .iter()
                .find(|chain| chain.gateway_addr_in_primary_chain == address)
                .expect("The gateway address of the secondary chain is not found, please check the configuration.")
                .finalizer_contract
                .as_ref()
                .expect("The finalizer_contract of secondary chain is not configured.")
        } else {
            &self.finalizer_contract
        }
    }

    pub fn l1_bridge(&self) -> &IL1Bridge<M> {
        if let Some(address) = self.selected_gate_way_address {
            self.second_chains
                .iter()
                .find(|chain| chain.gateway_addr_in_primary_chain == address)
                .expect("The gateway address of the secondary chain is not found, please check the configuration.")
                .l1_bridge
                .as_ref()
                .expect("The l1_bridge of secondary chain is not configured.")
        } else {
            &self.l1_bridge
        }
    }

    async fn synced_batch_number(&self) -> std::result::Result<U256, ContractError<M>> {
        if let Some(address) = self.selected_gate_way_address {
            self.second_chains
                .iter()
                .find(|chain| chain.gateway_addr_in_primary_chain == address)
                .expect("The gateway address of the secondary chain is not found, please check the configuration.")
                .zklink_contract
                .as_ref()
                .expect("The zklink_contract of secondary chain is not configured.")
                .get_total_batches_executed()
                .await
        } else {
            self.zksync_contract
                .get_total_batches_executed()
                .await
        }
    }

    /// [`Finalizer`] main loop.
    ///
    /// `M2` is expected to be an [`ZksyncMiddleware`] to connect to L2.
    pub async fn run<M2>(self, middleware: M2) -> Result<()>
    where
        M2: ZksyncMiddleware + 'static,
    {
        let params_fetcher_handle = tokio::spawn(params_fetcher_loop(
            self.pgpool.clone(),
            middleware,
            self.zksync_contract.clone(),
            self.l1_bridge().clone(),
            self.second_chains.clone(),
        ));

        use FinalizeWithdrawChain::*;
        let mut finalizer_handles = Vec::new();
        if self.finalize_withdraw_target == All || self.finalize_withdraw_target == SecondaryChain {
            for chain in self.second_chains.iter() {
                let mut finalizer = self.clone();
                finalizer.selected_gate_way_address = Some(chain.gateway_addr_in_primary_chain);
                finalizer_handles.push(tokio::spawn(finalizer.finalizer_loop()));
            }
        }

        if self.finalize_withdraw_target == All || self.finalize_withdraw_target == PrimaryChain {
            finalizer_handles.push(tokio::spawn(self.finalizer_loop()));
        }

        let (second_result, _, _) = futures::future::select_all(finalizer_handles).await;

        tokio::select! {
            m = params_fetcher_handle => {
                tracing::error!("migrator ended with {m:?}");
            }
            s = async { second_result } => {
                tracing::error!("A second finalizer ended with {s:?}");
            }
        }

        Ok(())
    }

    async fn predict_fails<'a, W: Iterator<Item = &'a WithdrawalParams>>(
        &self,
        withdrawals: W,
    ) -> Result<Vec<FinalizeResult>> {
        let w: Vec<_> = withdrawals
            .cloned()
            .map(|r| r.into_request_with_gaslimit(self.one_withdrawal_gas_limit))
            .collect();
        w.iter()
            .for_each(|w|
                tracing::info!(
                    "RequestFinalizeWithdrawal: \
                    l_2_block_number: {} \
                    l_2_message_index: {} \
                    l_2_tx_number_in_block: {} \
                    message: {} \
                    merkle_proof: {:?} \
                    ",
                    w.l_2_block_number,
                    w.l_2_message_index,
                    w.l_2_tx_number_in_block,
                    w.message,
                    w.merkle_proof.iter().map(|p| ethers::utils::hex::encode(p.as_ref())).collect::<Vec<_>>(),
                ));
        let results = self
            .finalizer_contract
            .finalize_withdrawals(w)
            .call()
            .await?;
        tracing::info!("predicted results for withdrawals: {results:?}");

        Ok(results
            .into_iter()
            .filter(|p| !p.success || p.gas > self.one_withdrawal_gas_limit)
            .collect())
    }

    async fn finalize_batch(&mut self, withdrawals: Vec<WithdrawalParams>) -> Result<()> {
        let Some(highest_batch_number) = withdrawals.iter().map(|w| w.l1_batch_number).max() else {
            return Ok(());
        };

        tracing::info!(
            "finalizing batch withdrawal_ids: {:?}",
            withdrawals.iter().map(|w| w.id).collect::<Vec<_>>()
        );

        let w: Vec<_> = withdrawals
            .iter()
            .cloned()
            .map(|r| r.into_request_with_gaslimit(self.one_withdrawal_gas_limit))
            .collect();

        let tx = self.finalizer_contract().finalize_withdrawals(w);
        let nonce = self
            .finalizer_contract()
            .client()
            .get_transaction_count(self.account_address, None)
            .await
            .map_err(|e| Error::Middleware(format!("{e}")))?;

        let tx = tx_sender::send_tx_adjust_gas(
            self.finalizer_contract().client(),
            tx.tx.clone(),
            self.tx_retry_timeout,
            nonce,
            self.batch_finalization_gas_limit,
        )
        .await;

        let ids: Vec<_> = withdrawals.iter().map(|w| w.id as i64).collect();

        // Turn actual withdrawals into info to update db with.
        let withdrawals = withdrawals.into_iter().map(|w| w.key()).collect::<Vec<_>>();

        match tx {
            Ok(Some(tx)) if tx.status.expect("EIP-658 is enabled; qed").is_zero() => {
                tracing::error!(
                    "withdrawal transaction {:?} was reverted",
                    tx.transaction_hash
                );

                FINALIZER_METRICS.reverted_withdrawal_transactions.inc();

                storage::inc_unsuccessful_finalization_attempts(&self.pgpool, &withdrawals).await?;

                return Err(Error::WithdrawalTransactionReverted);
            }
            Ok(Some(tx)) => {
                tracing::info!(
                    "withdrawal transaction {:?} successfully mined",
                    tx.transaction_hash
                );

                storage::finalization_data_set_finalized_in_tx(
                    &self.pgpool,
                    &withdrawals,
                    tx.transaction_hash,
                )
                .await?;

                FINALIZER_METRICS
                    .highest_finalized_batch_number
                    .set(highest_batch_number.as_u64() as i64);

                if let Some(ref mut withdrawals_meterer) = self.withdrawals_meterer {
                    if let Err(e) = withdrawals_meterer.meter_withdrawals_storage(&ids).await {
                        tracing::error!("Failed to meter the withdrawals: {e}");
                    }
                }
            }
            // TODO: why would a pending tx resolve to `None`?
            Ok(None) => {
                tracing::warn!("sent transaction resolved with none result",);
            }
            Err(e) => {
                tracing::error!(
                    "waiting for transaction status withdrawals failed with an error {:?}",
                    e
                );

                if let Some(provider_error) = e.as_provider_error() {
                    tracing::error!("failed to send finalization transaction: {provider_error}");
                } else if !is_gas_required_exceeds_allowance::<S>(&e) {
                    storage::inc_unsuccessful_finalization_attempts(&self.pgpool, &withdrawals)
                        .await?;
                } else {
                    tracing::error!("failed to send finalization withdrawal tx: {e}");
                    FINALIZER_METRICS
                        .failed_to_finalize_low_gas
                        .inc_by(withdrawals.len() as u64);

                    tokio::time::sleep(OUT_OF_FUNDS_BACKOFF).await;
                }
                // no need to bump the counter here, waiting for tx
                // has failed becuase of networking or smth, but at
                // this point tx has already been accepted into tx pool
            }
        }

        Ok(())
    }

    // Create a new withdrawal accumulator given the current gas price.
    async fn new_accumulator(&self) -> Result<WithdrawalsAccumulator> {
        let gas_price = self
            .finalizer_contract()
            .client()
            .get_gas_price()
            .await
            .map_err(|e| Error::Middleware(format!("{e}")))?;

        Ok(WithdrawalsAccumulator::new(
            gas_price,
            self.tx_fee_limit,
            self.batch_finalization_gas_limit,
            self.one_withdrawal_gas_limit,
        ))
    }

    async fn finalizer_loop(mut self)
    where
        S: Middleware,
        M: Middleware,
    {
        loop {
            if let Err(e) = self.loop_iteration().await {
                tracing::error!("iteration of finalizer loop has ended with {e}");
                tokio::time::sleep(LOOP_ITERATION_ERROR_BACKOFF).await;
            }
        }
    }

    async fn loop_iteration(&mut self) -> Result<()> {
        let total_executed_batches_number = self.synced_batch_number().await?.as_u32() as i64;
        tracing::info!("begin iteration of the finalizer loop: Batch Number({})", total_executed_batches_number);

        let try_finalize_these = match &self.token_list {
            TokenList::All => {
                storage::withdrawals_to_finalize(
                    &self.pgpool,
                    self.query_db_pagination_limit,
                    self.eth_threshold,
                    &self.selected_gate_way_address,
                    total_executed_batches_number
                )
                .await?
            }
            TokenList::WhiteList(w) => {
                storage::withdrawals_to_finalize_with_whitelist(
                    &self.pgpool,
                    self.query_db_pagination_limit,
                    w,
                    self.eth_threshold,
                    &self.selected_gate_way_address,
                    total_executed_batches_number
                )
                .await?
            }
            TokenList::BlackList(b) => {
                storage::withdrawals_to_finalize_with_blacklist(
                    &self.pgpool,
                    self.query_db_pagination_limit,
                    b,
                    self.eth_threshold,
                    &self.selected_gate_way_address,
                    total_executed_batches_number
                )
                .await?
            }
            TokenList::None => return Ok(()),
        };

        tracing::debug!("trying to finalize these {try_finalize_these:?}");

        if try_finalize_these.is_empty() {
            tokio::time::sleep(self.no_new_withdrawals_backoff).await;
            tracing::info!("There are currently no transactions that need to be finalized!");
            return Ok(());
        }

        let mut accumulator = self.new_accumulator().await?;
        let mut iter = try_finalize_these.into_iter().peekable();

        while let Some(t) = iter.next() {
            tracing::info!("Add tx[{}] to {:?}[{}] accumulator", t.tx_hash, self.which_chain_tx(&t.to_address), t.to_address);
            accumulator.add_withdrawal(t);

            if accumulator.ready_to_finalize() || iter.peek().is_none() {
                tracing::info!(
                    "predicting results for withdrawals: {:?}",
                    accumulator.withdrawals().map(|w| w.id).collect::<Vec<_>>()
                );

                let predicted_to_fail = self.predict_fails(accumulator.withdrawals()).await?;

                FINALIZER_METRICS
                    .predicted_to_fail_withdrawals
                    .inc_by(predicted_to_fail.len() as u64);


                if !predicted_to_fail.is_empty() {
                    tracing::warn!("predicted to fail: {predicted_to_fail:?}");
                    let mut removed = accumulator.remove_unsuccessful(&predicted_to_fail);

                    self.unsuccessful.append(&mut removed);
                }
            }

            if accumulator.ready_to_finalize() || iter.peek().is_none() {
                let requests = accumulator.take_withdrawals();
                self.finalize_batch(requests).await?;
                accumulator = self.new_accumulator().await?;
            }
        }

        self.process_unsuccessful().await
    }

    fn which_chain_tx(&self, target: &Address) -> FinalizeWithdrawChain {
        if self.second_chains.iter().any(|c| &c.gateway_addr_in_primary_chain == target) {
            FinalizeWithdrawChain::SecondaryChain
        } else {
            FinalizeWithdrawChain::PrimaryChain
        }
    }

    // process withdrawals that have been predicted as unsuccessful.
    //
    // there may be many reasons for such predictions for instance the following:
    // * a withdrawal is already finalized
    // * a gas limit on request was too low
    // * erc20 has denied a tx for some internal reasons.
    async fn process_unsuccessful(&mut self) -> Result<()> {
        if self.unsuccessful.is_empty() {
            tracing::debug!("no unsuccessful withdrawals");
            return Ok(());
        }

        let predicted = std::mem::take(&mut self.unsuccessful);
        tracing::debug!("requesting finalization status of withdrawals");
        let are_finalized =
            get_finalized_withdrawals(&predicted, &self.zksync_contract, self.l1_bridge(), &self.second_chains).await?;

        let mut already_finalized = vec![];
        let mut unsuccessful = vec![];

        for p in predicted {
            let key = p.key();

            if are_finalized.contains(&key) {
                already_finalized.push(key);
            } else {
                unsuccessful.push(key);
            }
        }

        tracing::debug!(
            "setting unsuccessful finalization attempts to {} withdrawals",
            unsuccessful.len()
        );

        // Either finalization tx has failed for these, or they were
        // predicted to fail.
        storage::inc_unsuccessful_finalization_attempts(&self.pgpool, &unsuccessful).await?;

        tracing::debug!(
            "setting already finalized status to {} withdrawals",
            already_finalized.len()
        );

        // if the withdrawal has already been finalized set its
        // finalization transaction to zero which is signals exactly this
        // it is known that withdrawal has been finalized but not known
        // in which exact transaction.
        //
        // this may happen in two cases:
        // 1. someone else has finalized it
        // 2. finalizer has finalized it however its course of
        // execution has been interrupted somewhere between the
        // submission of transaction and updating the db with the
        // result of said transaction success.
        storage::finalization_data_set_finalized_in_tx(
            &self.pgpool,
            &already_finalized,
            H256::zero(),
        )
        .await?;

        Ok(())
    }
}

async fn get_finalized_withdrawals<M, T: Clone>(
    withdrawals: &[WithdrawalParams],
    zksync_contract: &IZkSync<M>,
    l1_bridge: &IL1Bridge<M>,
    second_chains: &[SecondChainFinalizer<T, M>]
) -> Result<HashSet<WithdrawalKey>>
where
    M: Middleware + Clone,
{
    let results: Result<Vec<_>> =
        futures::future::join_all(withdrawals.iter().map(|wd| async move {
            let l1_batch_number = U256::from(wd.l1_batch_number.as_u64());
            let l2_message_index = U256::from(wd.l2_message_index);

            if is_eth(wd.sender) {
                if let Some(false) = wd.is_primary_chain {
                    second_chains
                        .iter()
                        .find(|chain| chain.gateway_addr_in_primary_chain == wd.to_address)
                        .unwrap()
                        .zklink_contract
                        .as_ref()
                        .expect("The zklink_contract of secondary chain is not configured.")
                        .is_eth_withdrawal_finalized(l1_batch_number, l2_message_index)
                } else {
                    zksync_contract
                        .is_eth_withdrawal_finalized(l1_batch_number, l2_message_index)
                }
                    .call()
                    .await
                    .map_err(|e| e.into())
            } else {
                if let Some(false) = wd.is_primary_chain {
                    second_chains
                        .iter()
                        .find(|chain| chain.gateway_addr_in_primary_chain == wd.to_address)
                        .unwrap()
                        .l1_bridge
                        .as_ref()
                        .expect("The l1_bridge of secondary chain is not configured.")
                } else {
                    l1_bridge
                }
                    .is_withdrawal_finalized(l1_batch_number, l2_message_index)
                    .call()
                    .await
                    .map_err(|e| e.into())
            }
        }))
        .await
        .into_iter()
        .collect();

    let results = results?;

    let mut set = HashSet::new();
    for i in 0..results.len() {
        if results[i] {
            set.insert(withdrawals[i].key());
        }
    }

    Ok(set)
}

fn is_gas_required_exceeds_allowance<M: Middleware>(e: &<M as Middleware>::Error) -> bool {
    if let Some(e) = e.as_error_response() {
        return e.code == -32000 && e.message.starts_with("gas required exceeds allowance ");
    }

    false
}

// Request finalization parameters for a set of withdrawals in parallel.
async fn request_finalize_params<M2>(
    pgpool: &PgPool,
    middleware: M2,
    hash_and_indices: &[(H256, u16, u64)],
    gate_way_addrs: &[Address]
) -> Vec<WithdrawalParams>
where
    M2: ZksyncMiddleware,
{
    let mut ok_results = Vec::with_capacity(hash_and_indices.len());

    // Run all parameter fetching in parallel.
    // Filter out errors and log them and increment a metric counter.
    // Return successful fetches.
    let res:Vec<Result<WithdrawalParams>> = futures::future::join_all(hash_and_indices.iter().map(|(h, i, id)| {
        middleware
            .finalize_withdrawal_params(*h, *i as usize)
            .map_ok(|r| {
                let mut r = r.expect("always able to ask withdrawal params; qed");
                r.id = *id;
                r
            })
            .map_err(crate::Error::from)
    }))
        .await;
    for (i, result) in res.into_iter().enumerate() {
        match result {
            Ok(r) => {
                if gate_way_addrs.contains(&r.to_address) {
                    let mut primary_withdraw_params = r.clone();
                    primary_withdraw_params.is_primary_chain = Some(true);
                    ok_results.push(primary_withdraw_params);
                    let mut second_withdraw_params = r;
                    second_withdraw_params.is_primary_chain = Some(false);
                    ok_results.push(second_withdraw_params);
                } else {
                    ok_results.push(r);
                }
            },
            Err(e) => {
                FINALIZER_METRICS.failed_to_fetch_withdrawal_params.inc();
                if let Error::Client(client::Error::WithdrawalLogNotFound(index, tx_hash)) = e {
                    storage::set_withdrawal_unfinalizable(pgpool, tx_hash, index)
                        .await
                        .ok();
                }
                tracing::error!(
                    "failed to fetch withdrawal parameters: {e} {:?}",
                    hash_and_indices[i]
                );
            }
        }
    }

    ok_results
}

// Continiously query the new withdrawals that have been seen by watcher
// request finalizing params for them and store this information into
// finalizer db table.
async fn params_fetcher_loop<M1, M2, T>(
    pool: PgPool,
    middleware: M2,
    zksync_contract: IZkSync<M1>,
    l1_bridge: IL1Bridge<M1>,
    second_chains: Vec<SecondChainFinalizer<T, M1>>,
) where
    T: Clone,
    M1: Middleware + Clone,
    M2: ZksyncMiddleware,
{
    loop {
        if let Err(e) =
            params_fetcher_loop_iteration(&pool, &middleware, &zksync_contract, &l1_bridge, &second_chains).await
        {
            tracing::error!("params fetcher iteration ended with {e}");
            tokio::time::sleep(LOOP_ITERATION_ERROR_BACKOFF).await;
        }
    }
}

async fn params_fetcher_loop_iteration<T: Clone, M1, M2>(
    pool: &PgPool,
    middleware: &M2,
    zksync_contract: &IZkSync<M1>,
    l1_bridge: &IL1Bridge<M1>,
    second_chains: &[SecondChainFinalizer<T, M1>],
) -> Result<()>
where
    M1: Middleware + Clone,
    M2: ZksyncMiddleware,
{
    let newly_executed_withdrawals = storage::get_withdrawals_with_no_data(pool, 1000).await?;

    if newly_executed_withdrawals.is_empty() {
        tokio::time::sleep(NO_NEW_WITHDRAWALS_BACKOFF).await;
        return Ok(());
    }

    tracing::debug!("newly committed withdrawals {newly_executed_withdrawals:?}");

    let hash_and_index_and_id: Vec<_> = newly_executed_withdrawals
        .iter()
        .map(|p| (p.key.tx_hash, p.key.event_index_in_tx as u16, p.id))
        .collect();

    let gate_way_addrs = second_chains
        .iter()
        .map(|chain| chain.gateway_addr_in_primary_chain)
        .collect::<Vec<_>>();
    let params = request_finalize_params(pool, &middleware, &hash_and_index_and_id, &gate_way_addrs).await;

    let already_finalized: Vec<_> = get_finalized_withdrawals(&params, zksync_contract, l1_bridge, second_chains)
        .await?
        .into_iter()
        .collect();

    storage::add_finalization_data(pool, &params).await?;
    storage::finalization_data_set_finalized_in_tx(pool, &already_finalized, H256::zero()).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::AddrList;

    use super::TokenList;
    use ethers::abi::Address;
    use pretty_assertions::assert_eq;

    #[test]
    fn tokens_list_de() {
        let all = "\"All\"";

        let none = "\"None\"";

        let all: TokenList = serde_json::from_str(all).unwrap();
        assert_eq!(all, TokenList::All);

        let none: TokenList = serde_json::from_str(none).unwrap();
        assert_eq!(none, TokenList::None);

        let black = r#"
            {
                "BlackList":[
                    "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4"
                ]
            }
        "#;

        let usdc_addr: Address = "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4"
            .parse()
            .unwrap();

        let blocked_usdc: TokenList = serde_json::from_str(black).unwrap();
        assert_eq!(blocked_usdc, TokenList::BlackList(vec![usdc_addr]));

        let white = r#"
            {
                "WhiteList":[
                    "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4"
                ]
            }
        "#;

        let allowed_usdc: TokenList = serde_json::from_str(white).unwrap();
        assert_eq!(allowed_usdc, TokenList::WhiteList(vec![usdc_addr]));
    }

    #[test]
    fn addr_list_de() {
        let addr_1: Address = "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4"
            .parse()
            .unwrap();
        let addr_2: Address = "0x1820495E7d1B8BA82B706FB972d2A2B8282023d0"
            .parse()
            .unwrap();

        let addr_list = r#"[
            "0x3355df6D4c9C3035724Fd0e3914dE96A5a83aaf4",
            "0x1820495E7d1B8BA82B706FB972d2A2B8282023d0"
        ]"#;

        let list: AddrList = AddrList::from_str(addr_list).unwrap();

        assert_eq!(list, AddrList(vec![addr_1, addr_2]));
    }
}
