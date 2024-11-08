{% docs evm_nft_events_table_doc %}

This table contains NFT sales on this EVM blockchain. More NFT marketplaces will be added over time.

{% enddocs %}


{% docs evm_ez_nft_sales_table_doc %}

This table contains NFT sale events from various marketplaces.

{% enddocs %}


{% docs evm_ez_nft_mint_table_doc %}

This table contains NFT mint events, defined as NFT transfers from a burn address to an address, on this EVM blockchain.

{% enddocs %}

{% docs evm_ez_nft_transfer_table_doc %}

This table contains NFT transfer events on this EVM blockchain.

{% enddocs %}

{% docs evm_lending_repayments_table_doc %}

This table contains repayment events for loans backed by collateralized NFTs on this EVM blockchain. Events in this table can be a full repayment either by the borrower to get their collateral back or by a new lender to repay the borrower's previous loan. The latter is labeled as refinance where the same borrower takes on a new loan from a new lender and repays the past loan. The NFT collateral is still locked in this scenario.

{% enddocs %}

{% docs evm_nft_aggregator_name %}

The name of the aggregator platform where the sale took place. If the sale did not take place via an aggregator platform, then the value will be null.

{% enddocs %}

{% docs evm_nft_amount %}

The total amount, specified by the mint token address, used as payment to mint the specified number of NFTs corresponding to this token id.

{% enddocs %}

{% docs evm_nft_amount_usd %}

The USD value of amount.

{% enddocs %}

{% docs evm_nft_block_no %}

The block number at which the NFT event occurred.

{% enddocs %}

{% docs evm_nft_blocktime %}

The block timestamp at which the NFT event occurred.

{% enddocs %}

{% docs evm_nft_buyer_address %}

The address of the buyer of the NFT in the transaction.

{% enddocs %}

{% docs evm_nft_creator_fee %}

The decimal adjusted amount of fees paid to the NFT collection as royalty payments for this NFT event in the transaction's currency.

{% enddocs %}

{% docs evm_nft_creator_fee_usd %}

The amount of fees paid to the NFT collection as royalty payments for this NFT event in US dollars.

{% enddocs %}

{% docs evm_nft_currency_address %}

The token contract address for this NFT event. This will be the native asset for native transactions.

{% enddocs %}

{% docs evm_nft_currency_symbol %}

The token symbol for this NFT event.

{% enddocs %}

{% docs evm_nft_erc1155_value %}

If the NFT is an ERC-1155 contract, this field may be one or greater, representing the number of tokens. If it is not an ERC-1155 token, this value will be null.

{% enddocs %}

{% docs evm_nft_event_index %}

The event number within a transaction.

{% enddocs %}

{% docs evm_nft_event_type %}

The type of NFT event in this transaction, either sale, bid_won, redeem, or mint.

{% enddocs %}

{% docs evm_nft_from_address %}

The sending address of the NFT in the transaction.

{% enddocs %}

{% docs evm_nft_intra_event_index %}

The order of events within a single event index. This is primarily used for ERC1155 NFT batch transfer events.

{% enddocs %}

{% docs evm_nft_metadata %}

The token metadata for this NFT. This may be blank for many NFTs. We are working to expand this field.

{% enddocs %}

{% docs evm_nft_mint_count %}

The number of NFTs minted in this event.

{% enddocs %}

{% docs evm_nft_mint_price %}

The price paid in the native asset to mint the NFT(s).

{% enddocs %}

{% docs evm_nft_mint_price_usd %}

The price paid in US dollars to mint the NFT(s).

{% enddocs %}

{% docs evm_nft_mints_symbol %}

The symbol of the token supplied to mint the NFT, if applicable. This field may not handle all edge cases.

{% enddocs %}

{% docs evm_nft_mints_token_address %}

The contract address of the token supplied to mint the NFT, if applicable. This field may not handle all edge cases.

{% enddocs %}

{% docs evm_nft_mints_token_price %}

The decimal adjusted amount of tokens supplied within the same transaction to mint the NFT. This field may not handle all edge cases.

{% enddocs %}

{% docs evm_nft_mints_token_price_usd %}

The amount of tokens supplied in US dollars within the same transaction to mint the NFT. This field may not handle all edge cases.

{% enddocs %}

{% docs evm_nft_nft_address %}

The contract address of the NFT.

{% enddocs %}

{% docs evm_nft_origin_from %}

The from address of this transaction. In most cases, this is the NFT buyer. However, for some more complex transactions, it may not be the NFT buyer.

{% enddocs %}

{% docs evm_nft_origin_sig %}

The function signature of this transaction.

{% enddocs %}

{% docs evm_nft_origin_to %}

The to address of this transaction. In most cases, this is the exchange contract. However, for some more complex NFT events, such as aggregate buys with tools, this may not be the exchange address.

{% enddocs %}

{% docs evm_nft_platform_address %}

The address of the exchange used for the transaction.

{% enddocs %}

{% docs evm_nft_platform_exchange_version %}

The version of the exchange contract used for the transaction.

{% enddocs %}

{% docs evm_nft_platform_fee %}

The decimal adjusted amount of fees paid to the platform for this NFT event in the transaction's currency. There are cases where there are fees paid to multiple marketplaces. In those cases, the fee in the platform_fee column will only reflect the platform fee related to the platform exchange contract.

{% enddocs %}

{% docs evm_nft_platform_fee_usd %}

The amount of fees paid to the platform for this NFT event in US dollars. There are cases where there are fees paid to multiple marketplaces. In those cases, the fee in the platform_fee column will only reflect the platform fee related to the platform exchange contract.

{% enddocs %}

{% docs evm_nft_platform_name %}

The name of the exchange used for the trade.

{% enddocs %}

{% docs evm_nft_price %}

The total price of the NFT, in the currency in which the transaction occurred and decimal adjusted where possible. Please note that the price of the NFT, after subtracting total fees, may not represent the net amount paid to the seller in all cases. You may refer to the platform fee description for more info.

{% enddocs %}

{% docs evm_nft_price_usd %}

The total price of the NFT in US dollars. This will be 0 for tokens without a decimal adjustment or hourly price. Please note that the price of the NFT, after subtracting total fees, may not represent the net amount paid to the seller in all cases. You may refer to the platform fee description for more info.

{% enddocs %}

{% docs evm_nft_project_name %}

The name of the NFT project. This field, along with metadata, will be filled in over time.

{% enddocs %}

{% docs evm_nft_seller_address %}

The address of the seller of the NFT in the transaction.

{% enddocs %}

{% docs evm_nft_to_address %}

The receiving address of the NFT in the transaction.

{% enddocs %}

{% docs evm_nft_tokenid %}

The token ID for this NFT contract.

{% enddocs %}

{% docs evm_nft_total_fees %}

The total amount of fees paid relating to the NFT purchase in the transaction currency. This includes royalty payments to creators and platform fees. Please note, this does not include the gas fee.

{% enddocs %}

{% docs evm_nft_total_fees_usd %}

The total amount of fees paid relating to the NFT purchase in US dollars. This includes royalty payments to creators and platform fees. Please note, this does not include the gas fee.

{% enddocs %}

{% docs evm_nft_tx_fee %}

The gas fee for this transaction in the native asset.

{% enddocs %}

{% docs evm_nft_tx_fee_usd %}

The gas fee for this transaction in US dollars.

{% enddocs %}

{% docs evm_nft_tx_hash %}

The transaction hash for the NFT event. This is not necessarily unique in this table as a transaction may contain multiple NFT events.

{% enddocs %}

{% docs evm_nft_lending_liquidations_table_doc %}

This table contains liquidation events for loans backed by collateralized NFTs on this EVM blockchain. This typically means the lender of the loan closes the loan and receives the NFT collateral.

{% enddocs %}

{% docs evm_platform_address %}

The contract address of the platform's lending contract.

{% enddocs %}

{% docs evm_platform_name %}

The name of the platform.

{% enddocs %}

{% docs evm_platform_exchange_version %}

The version of the platform's lending contract.

{% enddocs %}

{% docs evm_loanid %}

The ID associated with the loan, often times tied to the borrower's address and collateral. This ID can be used as a unique identifier when tracking active loans. However, note that refinances can happen while still maintaining the same loanId. For some platforms, loanids of previously paid loans are reused hence they can't be reliably used as a unique identifier without appropriate timestamp filters. Instead, use the unique_loan_id column.

{% enddocs %}

{% docs evm_unique_loan_id %}

The unique ID that can be used to represent a loan within the same platform. For the same loan, the value of this ID will remain the same across the loans taken, liquidations and repayment tables. For some platforms where the lender is issued a promissory note, the lender can send this note to any address, making the receiver the new lender. However, the unique_loan_id still remains the same although the final lender is different. The unique_loan_id changes only if the loanId and lender changes which would also indicate the previous loan has been fully paid off.

{% enddocs %}

{% docs evm_lender_address %}

The address that gives out a loan to the borrower. When a loan is issued, a promissory note (an ERC721) is issued to the lender. The lender can transfer this note to any address which makes any receiving address the new lender.

{% enddocs %}

{% docs evm_borrower_address %}

The address that receives the loan and has an NFT locked as collateral.

{% enddocs %}

{% docs evm_nft_address %}

The address of the NFT used as collateral.

{% enddocs %}

{% docs evm_tokenid %}

The tokenId of the NFT used as collateral.

{% enddocs %}

{% docs evm_loan_token_address %}

The contract address of what the loan is denominated in. This could be either the native asset or other tokens.

{% enddocs %}

{% docs evm_loan_token_symbol %}

The symbol of the loan token address.

{% enddocs %}

{% docs evm_principal_unadj %}

The principal amount of the loan taken with unadjusted decimal places.

{% enddocs %}

{% docs evm_principal %}

The principal amount of the loan taken with adjusted decimal places.

{% enddocs %}

{% docs evm_principal_usd %}

The principal amount of the loan taken in USD terms.

{% enddocs %}

{% docs evm_interest_rate %}

The non-annualized rate charged by the lender on the principal amount of the loan denominated in percentage (%). For fixed term loans, it is calculated by taking the difference between total debt and principal amount divided by principal amount. For perpetual term loans, interest rate is usually on an annual basis and total debt is calculated prorated.

{% enddocs %}

{% docs evm_apr %}

The annualized interest rate denominated in percentage (%).

{% enddocs %}

{% docs evm_loan_term_type %}

The type of loan terms, could be either a fixed loan or a perpetual loan. A fixed term loan has a fixed repayment date while a perpetual loan does not.

{% enddocs %}

{% docs evm_loan_start_timestamp %}

The timestamp of when the loan offer is accepted.

{% enddocs %}

{% docs evm_loan_due_timestamp %}

The timestamp of when the loan is due. For perpetual term loan, the value will be null.

{% enddocs %}

{% docs evm_loan_tenure %}

The tenure of the loan denominated in seconds. For perpetual term loan, the value will be null.

{% enddocs %}

{% docs evm_lending_loans %}

This table contains NFT lending events for loans backed by collateralized NFTs on this EVM blockchain. This could be depositing an NFT and taking out a loan or using the Buy Now Pay Later (BNPL) feature to take a loan out of the newly bought NFT. This NFT will remain locked until the loan is cleared.

{% enddocs %}

{% docs evm_nft_lending_event_type %}

There are 2 types of loan creation events. It can either be a new loan or a refinance of an existing loan. Note that renegotiation events are categorized as refinance events in this table. Old loans are repaid and new loan ids are reissued so these would be new loan entries in this table.

{% enddocs %}

{% docs evm_debt_unadj %}

The total debt of the loan (principal amount plus interests) with unadjusted decimal places.

{% enddocs %}

{% docs evm_debt %}

The total debt of the loan (principal amount plus interests) with adjusted decimal places.

{% enddocs %}

{% docs evm_debt_usd %}

The total debt of the loan (principal amount plus interests) in USD terms.

{% enddocs %}

{% docs evm_platform_fee_unadj %}

The fee charged by the lending platform with unadjusted decimal places.

{% enddocs %}

{% docs evm_platform_fee %}

The fee charged by the lending platform with adjusted decimal places.

{% enddocs %}

{% docs evm_platform_fee_usd %}

The fee charged by the lending platform in USD terms.

{% enddocs %}

{% docs evm_mev_arbitrage %}

This table contains the financial metrics from NFT arbitrage transactions including cost, revenue and profits.

{% enddocs %}

{% docs evm_mev_searcher %}

The address of the MEV finder, taken from the origin address that initiated the transaction.

{% enddocs %}

{% docs evm_mev_contract %}

The contract address that the MEV searcher uses to execute mev transactions, taken from the origin to address of the transaction.

{% enddocs %}

{% docs evm_cost_usd %}

The total cost in USD to purchase the NFT(s) in the transaction.

{% enddocs %}

{% docs evm_revenue_usd %}

The total revenue in USD from selling the NFT(s) and other related tokens in the transaction.

{% enddocs %}

{% docs evm_miner_tip_usd %}

The total amount in USD sent to the block miner/builder as a tip to include the transaction in the block.

{% enddocs %}

{% docs evm_profit_usd %}

The remainder in USD after subtracting cost, miner tip and transaction fee from revenue.

{% enddocs %}

{% docs evm_funding_source %}

The source of the funds used to perform the arbitrage. Currently there are three sources. Existing funds indicate that the user uses funds that are already in their balance while flash loan and flash swaps indicate that the user took a flash loan in the transaction.

{% enddocs %}

{% docs evm_arbitrage_direction %}

The direction in which the arbitrage is performed. There are two possibilities of origin and destination: pool and marketplace. Pool represents marketplaces where users swap from a pool whereas marketplace represents regular marketplaces that are not pool-based.

{% enddocs %}

{% docs evm_fact_mev_arbitrage_events %}

This table contains information about the NFTs involved in arbitrage transactions, documenting events from both the buy and sell sides.

{% enddocs %}

{% docs evm_trade_side %}

The side of the trade that this event represents. This can be either buy or sell. Buy would represent the events where NFTs are bought while sell would present the events where the bought NFTs are sold. Note that the number of events for buy and sell may not be the same for a given transaction.

{% enddocs %}

{% docs evm_dim_nft_collection_metadata %}

This table contains the metadata for popular NFT collections on this EVM blockchain.

{% enddocs %}

{% docs evm_nft_blockchain %}

Blockchain where the NFT metadata is retrieved from.

{% enddocs %}

{% docs evm_collection_name %}

The name for this NFT collection.

{% enddocs %}

{% docs evm_tokenid_name %}

The name for this specific tokenId. This may be the same for all tokenIds within the same collection.

{% enddocs %}

{% docs evm_traits %}

The traits for this tokenId in a key-value pair format.

{% enddocs %}

{% docs evm_tokenid_description %}

The description for this specific tokenId. This may be the same for all tokenIds within the same collection.

{% enddocs %}

{% docs evm_tokenid_image_url %}

The url of the image for this tokenId.

{% enddocs %}

{% docs evm_nft_address_tokenid %}

The concatenation of NFT address and tokenId.

{% enddocs %}