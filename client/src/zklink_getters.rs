#[allow(missing_docs)]
pub mod codegen {
    use ethers::prelude::abigen;

    abigen!(
        ZkLinkIGetters,
        "$CARGO_MANIFEST_DIR/src/contracts/IGetters.json"
    );
}
