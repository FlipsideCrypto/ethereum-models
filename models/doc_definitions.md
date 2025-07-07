{% docs evm_blocks_hash %}
Unique 66-character hash identifier for the block.

**Format**: 0x + 64 hexadecimal characters
**Usage**: 
- Primary identifier for block lookups
- Immutable once confirmed
- Used for block verification and linking

**Example**: `0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef`
{% enddocs %}

{% docs evm_contracts_name %}
The address of the smart contract that emitted the event or was involved in the transaction.

**Format**: 0x + 40 hexadecimal characters (42 characters total)
**Usage**:
- Identifies the specific contract involved
- Used for filtering and joining with contract metadata
- Essential for protocol-specific analysis

**Example**: `0x1234567890abcdef1234567890abcdef12345678`
{% enddocs %}

{% docs evm_eth_value %}
The amount of ETH (in wei) transferred in the transaction or trace.

**Format**: Raw integer value in wei (smallest unit of ETH)
**Usage**:
- Represents the exact amount of ETH transferred
- Used for value calculations and analysis
- 1 ETH = 10^18 wei

**Example**: `1000000000000000000` (1 ETH in wei)
{% enddocs %}

{% docs evm_precise_amount_unadjusted %}
The raw, unadjusted integer value of the asset amount (not divided by decimals).

**Usage**:
- Used for precise calculations and conversions
- To get the human-readable value, divide by the asset's decimals

**Example**: `1000000` (for a token with 6 decimals, this is 1.0)
{% enddocs %}

{% docs evm_precise_amount_adjusted %}
The human-readable value of the asset amount, adjusted for decimals.

**Usage**:
- Used for reporting and analytics
- Calculated as: raw_amount / (10^decimals)

**Example**: `1.0` (for a token with 6 decimals, raw value 1000000)
{% enddocs %}

{% docs evm_traces_gas %}
The amount of gas provided for the trace execution.
{% enddocs %}

{% docs evm_traces_gas_used %}
The amount of gas actually consumed by the trace execution.
{% enddocs %}

{% docs evm_traces_type %}
The type of trace (e.g., call, create, suicide, reward).
{% enddocs %}

{% docs evm_trace_address %}
The hierarchical address of the trace within the transaction, used to identify nested calls.
{% enddocs %}

{% docs evm_sub_traces %}
The number of sub-traces (internal calls) within this trace.
{% enddocs %}

{% docs evm_tx_succeeded %}
Boolean indicating if the transaction succeeded (true) or failed (false).
{% enddocs %}

{% docs evm_trace_succeeded %}
Boolean indicating if the trace succeeded (true) or failed (false).
{% enddocs %}

{% docs evm_traces_input %}
The input data sent to the contract in the trace.
{% enddocs %}

{% docs evm_traces_output %}
The output data returned by the contract in the trace.
{% enddocs %}

{% docs evm_nft_nft_address %}
The address of the NFT contract (ERC-721 or ERC-1155).

**Format**: 0x + 40 hexadecimal characters (42 characters total)
**Usage**:
- Identifies the NFT collection
- Used for filtering and joining with NFT metadata

**Example**: `0x1234567890abcdef1234567890abcdef12345678`
{% enddocs %}

{% docs evm_nft_token_id %}
The unique identifier for the NFT within the collection.

**Usage**:
- Used to distinguish individual NFTs in a contract
- Always an integer (uint256)

**Example**: `12345`
{% enddocs %}

{% docs evm_nft_owner_address %}
The address of the current owner of the NFT.

**Format**: 0x + 40 hexadecimal characters
**Usage**:
- Used for ownership tracking and transfer analysis

**Example**: `0xabcdefabcdefabcdefabcdefabcdefabcdefabcd`
{% enddocs %}

{% docs evm_attestations_table_doc %}
This table contains beacon chain attestation data from the Ethereum consensus layer.
{% enddocs %}

{% docs evm_beacon_blocks_table_doc %}
This table contains beacon chain block data from the Ethereum consensus layer.
{% enddocs %}

{% docs evm_beacon_deposits_table_doc %}
This table contains beacon chain deposit data from the Ethereum consensus layer.
{% enddocs %}

{% docs evm_beacon_validator_balances_table_doc %}
This table contains beacon chain validator balance data from the Ethereum consensus layer.
{% enddocs %}

{% docs evm_beacon_validators_doc %}
This table contains beacon chain validator data from the Ethereum consensus layer.
{% enddocs %}

{% docs evm_blockchain %}
The blockchain network identifier (e.g., 'ethereum', 'polygon').
{% enddocs %}

{% docs evm_current_bal_table_doc %}
This table contains current balance data for addresses and tokens.
{% enddocs %}

{% docs evm_diffs_table_doc %}
This table contains balance difference data showing changes over time.
{% enddocs %}

{% docs evm_epoch_number %}
The epoch number in the beacon chain consensus mechanism.
{% enddocs %}

{% docs evm_internal_column %}
Internal column used for data processing and joins.
{% enddocs %}

{% docs evm_log_id_events %}
Unique identifier for event logs in the blockchain.
{% enddocs %}

{% docs evm_lp_actions_action %}
The type of liquidity provider action (e.g., 'add', 'remove', 'collect').
{% enddocs %}

{% docs evm_lp_actions_table_doc %}
This table contains liquidity provider action data for DEX protocols.
{% enddocs %}

{% docs evm_token_standard %}
The token standard (e.g., 'ERC-20', 'ERC-721', 'ERC-1155').
{% enddocs %}

{% docs evm_tx_status %}
The status of the transaction (e.g., 'success', 'failed').
{% enddocs %}

{% docs evm_nft_origin_sig %}
The function signature that originated the NFT transaction.
{% enddocs %}

{% docs evm_nft_platform_exchange_version %}
The version of the NFT platform/exchange used for the transaction.
{% enddocs %}

{% docs evm_nft_project_name %}
The name of the NFT project or collection.
{% enddocs %}

{% docs evm_nft_tx_fee %}
The transaction fee paid for the NFT transaction.
{% enddocs %}

{% docs evm_nft_tx_fee_usd %}
The transaction fee paid for the NFT transaction in USD.
{% enddocs %}

{% docs ez_nft_sales_event_index %}
The index of the event log within the transaction.
{% enddocs %}

{% docs ez_nft_sales_contract_address %}
The address of the NFT contract (ERC-721 or ERC-1155).
{% enddocs %}

{% docs ez_nft_sales_token_id %}
The unique identifier for the NFT within the collection.
{% enddocs %}

{% docs ez_nft_sales_buyer_address %}
The address of the buyer in the NFT sale.
{% enddocs %}

{% docs ez_nft_sales_seller_address %}
The address of the seller in the NFT sale.
{% enddocs %}

{% docs ez_nft_sales_platform_name %}
The name of the NFT marketplace or platform.
{% enddocs %}

{% docs ez_nft_sales_event_type %}
The type of NFT event (e.g., 'Sale', 'Bid', 'Transfer').
{% enddocs %}

{% docs ez_token_transfers_contract_address %}
The address of the token contract for the transfer.
{% enddocs %}

{% docs fact_event_logs_event_index %}
The index of the event log within the transaction.
{% enddocs %}

{% docs ez_decoded_event_logs_event_name %}
The name of the decoded event from the contract ABI.
{% enddocs %}

{% docs fact_transactions_eth_value %}
The amount of ETH (in wei) transferred in the transaction.
{% enddocs %}

{% docs fact_traces_gas %}
The amount of gas provided for the trace execution.
{% enddocs %}

{% docs fact_traces_gas_used %}
The amount of gas actually consumed by the trace execution.
{% enddocs %}

{% docs fact_traces_type %}
The type of trace (e.g., call, create, suicide, reward).
{% enddocs %}

{% docs fact_traces_trace_address %}
The hierarchical address of the trace within the transaction.
{% enddocs %}

{% docs fact_traces_sub_traces %}
The number of sub-traces (internal calls) within this trace.
{% enddocs %}

{% docs fact_traces_input %}
The input data sent to the contract in the trace.
{% enddocs %}

{% docs fact_traces_output %}
The output data returned by the contract in the trace.
{% enddocs %} 