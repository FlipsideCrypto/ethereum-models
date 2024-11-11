{% docs evm_column_deprecation_notice %}

This column is being deprecated for standardization purposes. Please update your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_hash %}

This column is being deprecated for standardization purposes. Please update your queries to use the `block_hash` column instead by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_block_hash %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}


{% docs evm_column_deprecation_notice_position %}

This column is being deprecated for standardization purposes. Please update your queries to use the `tx_position` column instead by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_tx_status %}

This column is being deprecated for standardization purposes. Please update your queries to use the `tx_succeeded` column instead by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_chain_id %}

This column is being removed for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_identifier %}

This column is being deprecated for standardization purposes. Please update your queries to use the `trace_address` column instead by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_trace_status %}

This column is being deprecated for standardization purposes. Please update your queries to use the `trace_succeeded` column instead by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_log_id %}

This column is being removed for standardization purposes. Please remove or update your queries with `CONCAT(tx_hash, '_', event_index)` to reconstruct `_log_id` by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_blockchain %}

This column is being removed for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_block_header_json %}

This column is being removed for standardization purposes and will be replaced with new columns associated with the keys extracted from this json. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_full_decoded_log %}

This column is being deprecated for standardization purposes. Please update your queries to use the `full_decoded_data` column instead by February 3, 2025.

{% enddocs %}

{% docs evm_table_deprecation_notice %}

This table is being deprecated for standardization purposes. Please update your queries by February 3, 2025.

{% enddocs %}

{% docs evm_table_deprecation_notice_fact_decoded_event_logs %}

This table is being deprecated for standardization purposes. Please update your queries to use `ez_decoded_event_logs` instead by February 3, 2025.

{% enddocs %}

{% docs evm_table_deprecation_notice_fact_decoded_traces %}

This table is being deprecated for standardization purposes. Please update your queries to use `ez_decoded_traces` instead by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_state_root_tx_hash %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_state_root_batch_index %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_submission_tx_hash %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_submission_batch_index %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_submission_details %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_withdrawals_root %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_deposit_nonce %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_deposit_receipt_version %}

This column is being deprecated for standardization purposes. Please remove from your queries by February 3, 2025.

{% enddocs %}