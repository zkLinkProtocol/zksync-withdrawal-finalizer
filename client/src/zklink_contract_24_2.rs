#[allow(missing_docs)]
pub mod codegen {
    use ethers::prelude::abigen;

    abigen!(
        IZkSync,
        "$CARGO_MANIFEST_DIR/src/contracts/24_2_IZkSync.json"
    );
}
