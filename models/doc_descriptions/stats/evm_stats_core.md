{% docs evm_ez_core_metrics_hourly_table_doc %}

A convenience table that aggregates block and transaction related metrics using various aggregate functions such as SUM, COUNT, MIN and MAX from the fact_transactions table, on an hourly basis.

{% enddocs %}

{% docs evm_block_timestamp_hour %}

The hour of the timestamp of the block.

{% enddocs %}

{% docs evm_block_number_min %}

The minimum block number in the hour.

{% enddocs %}

{% docs evm_block_number_max %}

The maximum block number in the hour.

{% enddocs %}

{% docs evm_block_count %}

The number of blocks in the hour.

{% enddocs %}

{% docs evm_transaction_count %}

The number of transactions in the hour.

{% enddocs %}

{% docs evm_transaction_count_success %}

The number of successful transactions in the hour.

{% enddocs %}

{% docs evm_transaction_count_failed %}

The number of failed transactions in the hour.

{% enddocs %}

{% docs evm_unique_from_count %}

The number of unique origin from addresses in the hour.

{% enddocs %}

{% docs evm_unique_to_count %}

The number of unique origin to addresses in the hour.

{% enddocs %}

{% docs evm_total_fees_native %}

The sum of all fees in the hour, in the native fee currency. Note, Snowflake's native SUM() function may limit decimal precision. To view precise transaction fee values, please use the `tx_fee_precise` column in `core.fact_transactions`.

{% enddocs %}

{% docs evm_total_fees_usd %}

The sum of all fees in the hour, in USD, rounded to 2 decimal places.

{% enddocs %}