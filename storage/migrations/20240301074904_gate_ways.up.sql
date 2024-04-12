CREATE TABLE second_chain_gate_ways (
    id BIGSERIAL PRIMARY KEY,
    gate_way_addr BYTEA NOT NULL,
    l1_block_number BIGINT NOT NULL,
    sync_batch_number BIGINT NOT NULL
);
