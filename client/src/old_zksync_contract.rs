
#[allow(missing_docs)]
pub mod codegen {
    use ethers::prelude::abigen;

    abigen!(OldIZkSync, "$CARGO_MANIFEST_DIR/src/contracts/OldIZkSync.json");
}


#[cfg(test)]
mod tests {
    use crate::zksync_contract::parse_withdrawal_events_l1;
    use ethers::abi::{AbiDecode, Address, Bytes};
    use hex::FromHex;
    use std::str::FromStr;
    use crate::old_zksync_contract::codegen::CommitBatchesCall;

    #[test]
    fn parse_l2_to_l1() {
        let input = include_str!("../../test_tx.txt");
        let bytes = Bytes::from_hex(input).unwrap();
        let commit_batches = CommitBatchesCall::decode(bytes).unwrap();
        let pubdata = commit_batches.new_batches_data
            .into_iter()
            .map(|batch| {
                (batch.batch_number, batch.total_l2_to_l1_pubdata.into())
            })
            .collect::<Vec<_>>();
        let withdrawals = parse_withdrawal_events_l1(
            pubdata,
            0,
            Address::from_str("11f943b2c77b743AB90f4A0Ae7d5A4e7FCA3E102").unwrap(),
        );
        assert_eq!(withdrawals.len(), 19);
    }
}