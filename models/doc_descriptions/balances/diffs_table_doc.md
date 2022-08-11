{% docs diffs_table_doc %}

This table contains the block level balance changes for both tokens (including ERC721s) and Native ETH on the Ethereum Blockchain for all wallets and contracts. If a token or Native ETH is moved, we will read the balance of the involved wallets at that block, and carry forward the previous balance into the current record. Symbol, name, and price are joined where possible. ERC721s are included. Data is currently read at the max daily block for wallets with activity. We plan to increase the granularity on this table to block level changes in the near term. 

Please note - the underlying data for this is large. If you want your query to run quickly, please use filters as much as possible. Using at least `last_activity_block_timestamp::date` as a filter will lead to optimal query performance.

{% enddocs %}