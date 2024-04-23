#[allow(missing_docs)]
pub mod codegen {
    use ethers::prelude::abigen;

    abigen!(ZkLink, "$CARGO_MANIFEST_DIR/src/contracts/ZkLink.json");
}
