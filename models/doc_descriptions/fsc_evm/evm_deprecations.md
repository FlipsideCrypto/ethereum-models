{% docs evm_column_deprecation_notice %}

This column is being deprecated for standardization purposes. Please update your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_hash %}

This column is being deprecated for standardization purposes. Please update your queries to use the `block_hash` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_block_hash %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}


{% docs evm_column_deprecation_notice_position %}

This column is being deprecated for standardization purposes. Please update your queries to use the `tx_position` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_tx_status %}

This column is being deprecated for standardization purposes. Please update your queries to use the `tx_succeeded` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_chain_id %}

This column is being removed for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_identifier %}

This column is being deprecated for standardization purposes. Please update your queries to use the `trace_address` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_trace_status %}

This column is being deprecated for standardization purposes. Please update your queries to use the `trace_succeeded` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_log_id %}

This column is being removed for standardization purposes. Please remove or update your queries with `CONCAT(tx_hash, '_', event_index)` to reconstruct `_log_id` by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_inserted_timestamp %}

This column is being removed for standardization purposes. Please update your queries to use the `modified_timestamp` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_blockchain %}

This column is being removed for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_block_header_json %}

This column is being removed for standardization purposes and will be replaced with new columns associated with the keys extracted from this json. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_table_deprecation_notice %}

This table is being deprecated for standardization purposes. Please update your queries by March 10, 2025.

{% enddocs %}

{% docs evm_table_deprecation_notice_fact_decoded_event_logs %}

This table is being deprecated for standardization purposes. Please update your queries to use `ez_decoded_event_logs` instead by March 10, 2025.

{% enddocs %}

{% docs evm_table_deprecation_notice_fact_decoded_traces %}

This table is being deprecated for standardization purposes. Please update your queries to use `ez_decoded_traces` instead by March 10, 2025.

{% enddocs %}

{% docs evm_table_deprecation_notice_fact_token_transfers %}

This table is being deprecated for standardization purposes. Please update your queries to use `ez_token_transfers` instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_state_root_tx_hash %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_state_root_batch_index %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_submission_tx_hash %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_submission_batch_index %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_l1_submission_details %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_data %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_deposit_nonce %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_deposit_receipt_version %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_token_price %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_has_price %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_has_decimal %}

This column is being deprecated for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_nft_address %}

This column is being removed for standardization purposes. Please update your queries to use the `contract_address` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_tokenid %}

This column is being removed for standardization purposes. Please update your queries to use the `token_id` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_erc1155_value %}

This column is being removed for standardization purposes. Please update your queries to use the `quantity` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_project_name %}

This column is being removed for standardization purposes. Please update your queries to use the `name` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_loanid %}

This column is being removed for standardization purposes. Please update your queries to use the `loan_id` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_tokenid_name %}

This column is being removed for standardization purposes. Please update your queries to use the `token_id_name` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_tokenid_description %}

This column is being removed for standardization purposes. Please update your queries to use the `token_id_description` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_tokenid_image_url %}

This column is being removed for standardization purposes. Please update your queries to use the `token_id_image_url` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_nft_address_tokenid %}

This column is being removed for standardization purposes. Please update your queries to use the `nft_address_token_id` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_nft_from_address %}

This column is being removed for standardization purposes. Please update your queries to use the `from_address` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_nft_to_address %}

This column is being removed for standardization purposes. Please update your queries to use the `to_address` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_mint_price_eth %}

This column is being removed for standardization purposes. Please update your queries to use the `mint_price_native` column instead by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_event_type %}

This column is being removed for standardization purposes. Please remove from your queries by March 10, 2025.

{% enddocs %}

{% docs evm_column_deprecation_notice_fact_decoded_traces_id %}

This column is being removed for standardization purposes. Please update your queries to use the `ez_decoded_traces_id` column instead by March 10, 2025.

{% enddocs %}
