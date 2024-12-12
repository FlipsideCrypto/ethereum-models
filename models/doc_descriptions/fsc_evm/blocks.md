{% docs evm_blocks_table_doc %}

This table contains block level data for this EVM blockchain. This table can be used to analyze trends at a block level, for example gas fees vs. total transactions over time. For more information, please see [Etherscan Resources](https://etherscan.io/directory/Learning_Resources/Ethereum) or [The Ethereum Organization](https://ethereum.org/en/developers/docs/blocks/)

{% enddocs %}


{% docs evm_block_header_json %}

This JSON column contains the block header details.

{% enddocs %}


{% docs evm_blockchain %}

The blockchain on which transactions are being confirmed.

{% enddocs %}


{% docs evm_blocks_hash %}

The hash of the block header for a given block.

{% enddocs %}


{% docs evm_blocks_nonce %}

Block nonce is a value used during mining to demonstrate proof of work for a given block.

{% enddocs %}


{% docs evm_difficulty %}

The effort required to mine the block.

{% enddocs %}


{% docs evm_extra_data %}

Any data included by the miner for a given block.

{% enddocs %}


{% docs evm_gas_limit %}

Total gas limit provided by all transactions in the block.

{% enddocs %}


{% docs evm_gas_used %}

Total gas used in the block.

{% enddocs %}

{% docs evm_network %}

The network on the blockchain used by a transaction.

{% enddocs %}


{% docs evm_parent_hash %}

The hash of the block from which a given block is generated. Also known as the parent block.

{% enddocs %}


{% docs evm_receipts_root %}

The root of the state trie.

{% enddocs %}


{% docs evm_sha3_uncles %}

The mechanism which Ethereum Javascript RLP encodes an empty string.

{% enddocs %}


{% docs evm_size %}

Block size, which is determined by a given block's gas limit.

{% enddocs %}


{% docs evm_total_difficulty %}

Total difficulty of the chain at a given block.

{% enddocs %}


{% docs evm_tx_count %}

Total number of transactions within a block.

{% enddocs %}


{% docs evm_uncle_blocks %}

Uncle blocks occur when two blocks are mined and broadcasted at the same time, with the same block number. The block validated across the most nodes will be added to the primary chain, and the other one becomes an uncle block. Miners do receive rewards for uncle blocks.

{% enddocs %}

{% docs evm_miner %}

The address of the beneficiary to whom the mining rewards were given

{% enddocs %}

{% docs evm_state_root %}

The root of the final state trie of the block

{% enddocs %}

{% docs evm_transactions_root %}

The root of the transaction trie of the block

{% enddocs %}

{% docs evm_logs_bloom %}

The bloom filter for the logs of the block.

{% enddocs %}

{% docs evm_mix_hash %}

A string of a 256-bit hash encoded as a hexadecimal

{% enddocs %}

{% docs evm_base_fee_per_gas %}

A string of the base fee encoded in hexadecimal format. Please note that this response field will not be included in a block requested before the EIP-1559 upgrade

{% enddocs %}

{% docs evm_withdrawals_root %}

The root of the withdrawals trie.

{% enddocs %}