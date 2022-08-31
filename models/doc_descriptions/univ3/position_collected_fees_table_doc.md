{% docs position_collected_fees_table_doc %}

Fees collected by a Liquidity Provider (LP) on their position. In V3 fees are accrued and collected in each token within the pair. When a pool Collect event is emitted a new record is appended to this table. If a Burn event is emitted in the same transaction as the Collect event the amount of the burn is subtracted from the Collect event token1 and token0 amounts. This allows us to arrive solely at the swap fees collected.

{% enddocs %}