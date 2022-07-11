{% docs eth_ez_sushi_swaps_table_doc %}

This table will contain all swap events done on the Sushiswap platform in the ```fact_event_logs table```, joined along with the addresses of both tokens in the swap and the decimal adjusted prices.
Note: A rule has been put in place to null out the amount_USD if that number is too divergent between amount_in_USD and amount_out_usd. This can happen for swaps of less liquid tokens during very high fluctuation of price.

{% enddocs %}