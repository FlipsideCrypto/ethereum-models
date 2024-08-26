{% docs eth_dex_swaps_amount_in_usd %}

The value of the swapped tokens in USD at the time of the transaction, where available. If there is a variance of 75% or greater in USD swap values or one side of the swap's USD value is NULL, then both amount_in_usd and amount_out_usd will be set to NULL. This avoids falsely overstating USD swap volumes for low-liquidity token pairings or incorrectly reported prices. To get USD values in these scenarios anyway, please re-join the `price.ez_prices_hourly` table. Note, this logic does not apply for the chain's Native asset. 

{% enddocs %}