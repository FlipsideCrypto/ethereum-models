{% docs eth_ez_eth_transfers_table_doc %}

This table contains all native ETH transfers, including equivalent USD amounts. The origin addresses correspond to the to and from addresses from the `fact_transactions` table. The `identifier` and `tx_hash` columns relate this table back to `fact_traces`, which contains more details on the transfers.

{% enddocs %}