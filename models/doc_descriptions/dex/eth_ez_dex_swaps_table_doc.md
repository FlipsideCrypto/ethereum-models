{% docs eth_ez_dex_swaps_table_doc %}

This table currently contains swap events from the ```fact_event_logs``` table for SUSHI, UNISWAP, CURVE, SYNTHETIX, BALANCER, DODO, FRAX, HASHFLOW, KYBERSWAP, MAVERICK, PANCAKESWAP, SHIBASWAP, TRADER JOE AND VERSE along with other helpful columns including an amount USD where possible. Other dexes coming soon! 
Note: A rule has been put in place to null out the amount_USD if that number is too divergent between amount_in_USD and amount_out_usd. This can happen for swaps of less liquid tokens during very high fluctuation of price.

{% enddocs %}