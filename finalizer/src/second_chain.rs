use ethers::abi::Address;
use client::l1bridge::codegen::IL1Bridge;
use client::withdrawal_finalizer::codegen::WithdrawalFinalizer;
use client::zklink_contract::codegen::ZkLink;

#[derive(Clone)]
pub struct SecondChainFinalizer<M1, M2> where
    M1: Clone,
    M2: Clone,
{
    pub finalizer_contract: WithdrawalFinalizer<M1>,
    pub zklink_contract: ZkLink<M2>,
    pub l1_bridge: IL1Bridge<M2>,
    pub gateway_address: Address,
}