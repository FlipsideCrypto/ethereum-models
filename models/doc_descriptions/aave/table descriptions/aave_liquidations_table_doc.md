{% docs aave_liquidations_table_doc %}

Aave.ez_liquidatons contains transactions in which a borrower's collateral asset is liquidated. Liquidations occur when a borrower's health factor goes below 1 due to their collateral value not properly covering their loan/debt value. This might happen when the collateral decreases in value or the borrowed debt increases in value against each other. This collateral vs loan value ratio is shown in the health factor. In a liquidation, up to 50% of a borrower's debt is repaid and that value + liquidation fee is taken from the collateral available, so after a liquidation the amount liquidated from one's debt is repaid. 

{% enddocs %}