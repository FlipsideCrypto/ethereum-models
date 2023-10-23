{% docs eth_ez_eth_transfers_table_doc %}

Deprecating soon! Please migrate queries to the `ez_native_transfers` table. The new table contains the same data, but uses slightly different naming conventions in order to support multiple native assets for the upcoming `MATIC` -> `POL` migration.

{% enddocs %}

{% docs eth_ez_native_transfers_table_doc %}

This table contains all native asset transfers, including equivalent USD amounts. Delegate calls are excluded, and only successful traces and transactions are included. The origin addresses correspond to the `to_address` and `from_address` in the `fact_transactions` table. The `identifier` and `tx_hash` columns relate this table back to `fact_traces`, which contains more details on the transfers. Please note this table is replacing the `ez_eth_transfers` table. There are some minor differences in naming conventions, but the data is the same.

{% enddocs %}