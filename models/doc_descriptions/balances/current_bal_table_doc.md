{% docs current_bal_table_doc %}

This table contains the current, non 0, balances for wallets on the Ethereum Blockchain. Symbol, name, and price are joined where possible. Prices are calculated as of the last activity date and as of the most recently recorded hourly price. ERC721s are included.

Please note - the underlying data for this is large. If you want your query to run quickly, please use filters as much as possible. Using at least `last_activity_block_timestamp::date` as a filter will lead to optimal query performance.

{% enddocs %}