{% docs evm_fact_token_transfers_table_doc %}

This fact-based table contains emitted event logs for ERC-20 Token Transfers (e.g. `Transfer`: topic_0 = `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`). The contract address is the token transferred, and the raw amount field is the amount of tokens transferred. The values in this table are not decimal adjusted, instead please use `core.dim_contracts` or `core.ez_token_transfers` to reference decimals or decimal adjusted values. This table does not contain ERC-721 and ERC-1155 token transfers, instead please use `nft.ez_nft_transfers`. Additionally, this table does not contain transfers of the chain's native asset, instead please use `core.ez_native_transfers`.

{% enddocs %}

{% docs evm_ez_token_transfers_table_doc %}

This convenience table contains emitted event logs for ERC-20 Token Transfers (e.g. `Transfer`: topic_0 = `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`), including decimal adjusted values, usd values, and other helpful token metadata where available for each transfer event. The contract address is the token transferred, and the raw amount field is the amount of tokens transferred. Note, this table does not contain ERC-721 and ERC-1155 token transfers, instead please use `nft.ez_nft_transfers`. Additionally, this table does not contain transfers of the chain's native asset, instead please use `core.ez_native_transfers`.

{% enddocs %}

{% docs evm_ez_native_transfers_table_doc %}

This convenience table contains all transfers for the chain's native asset, sourced from internal traces (`core.fact_traces`), and includes decimal adjusted and usd values where available. The origin addresses correspond to the to and from addresses in the `core.fact_transactions` table. Note, this table does not contain ERC-721 and ERC-1155 token transfers, instead please use `nft.ez_nft_transfers`.

{% enddocs %}