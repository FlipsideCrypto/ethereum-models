{% docs eth_transfer_table_doc %}

This table contains events on the Ethereum Blockchain with an event name of 'Transfer'. The contract address is the token transferred, and the raw amount field is the amount of tokens transferred. Please note this amount is not decimal adjusted. Decimal adjustments for a token address can be found in ```dim_contracts```. This table will not contain transfers of native ETH.

{% enddocs %}