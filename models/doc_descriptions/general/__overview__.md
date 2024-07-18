{% docs __overview__ %}

# Welcome to the Flipside Crypto Ethereum Models Documentation!

## **What does this documentation cover?**
The documentation included here details the design of the Ethereum tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/earn) For more information on how these models are built, please see [the github repository.](https://github.com/FlipsideCrypto/ethereum-models)

## **How do I use these docs?**
The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (ethereum.core)

**Dimension Tables:**
- [dim_contract_abis](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__dim_contract_abis)
- [dim_contracts](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__dim_contracts)
- [dim_event_signatures](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__dim_event_signatures)
- [dim_function_signatures](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__dim_function_signatures)
- [dim_labels](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__dim_labels)

**Fact Tables:**
- [fact_blocks](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_blocks)
- [fact_contract_reads](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_contract_reads)
- [fact_decoded_event_logs](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_decoded_event_logs)
- [fact_decoded_traces](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_decoded_traces)
- [fact_eth_balances](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_eth_balances)
- [fact_event_logs](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_event_logs)
- [fact_token_balances](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_token_balances)
- [fact_token_transfers](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_token_transfers)
- [fact_traces](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_traces)
- [fact_transactions](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_transactions)

**Convenience Tables:**
- [ez_balance_deltas](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_balance_deltas)
- [ez_current_balances](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_current_balances)
- [ez_decoded_event_logs](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_decoded_event_logs)
- [ez_decoded_traces](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_decoded_traces)
- [ez_native_transfers](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_native_transfers)
- [ez_token_transfers](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_token_transfers)

### Price Tables (ethereum.price)
- [dim_asset_metadata](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.price__dim_asset_metadata)
- [fact_prices_ohlc_hourly](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.price__fact_prices_ohlc_hourly)
- [ez_asset_metadata](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.price__ez_asset_metadata)
- [ez_prices_hourly](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.price__ez_prices_hourly)

### DeFi Tables (ethereum.defi)
- [ez_dex_swaps](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_dex_swaps)
- [dim_dex_liquidity_pools](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__dim_dex_liquidity_pools)
- [ez_liquid_staking_deposits](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_liquid_staking_deposits)
- [ez_liquid_staking_withdrawals](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_liquid_staking_withdrawals)
- [ez_bridge_activity](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_bridge_activity)
- [ez_lending_borrows](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_lending_borrows) 
- [ez_lending_deposits](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_lending_deposits)
- [ez_lending_flashloans](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_lending_flashloans)
- [ez_lending_liquidations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_lending_liquidations)
- [ez_lending_repayments](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_lending_repayments)
- [ez_lending_withdraws](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.defi__ez_lending_withdraws)

### NFT Tables (ethereum.nft)
- [dim_nft_collection_metadata](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__dim_nft_collection_metadata)
- [ez_nft_mints](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__ez_nft_mints)
- [ez_nft_sales](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__ez_nft_sales)
- [ez_nft_transfers](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__ez_nft_transfers)
- [ez_lending_liquidations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__ez_lending_liquidations)
- [ez_lending_loans](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__ez_lending_loans)
- [ez_lending_repayments](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__ez_lending_repayments)
- [ez_mev_arbitrage](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__ez_mev_arbitrage)
- [fact_mev_arbitrage_events](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.nft__fact_mev_arbitrage_events)

### Stats Tables (ethereum.stats)
- [ez_core_metrics_hourly](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.stats__ez_core_metrics_hourly)

### Aave Tables (ethereum.aave)
- [ez_borrows](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_borrows) 
- [ez_deposits](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_deposits)
- [ez_flashloans](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_flashloans)
- [ez_liquidations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_liquidations)
- [ez_market_stats](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_market_stats)
- [ez_proposals](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_proposals)
- [ez_repayments](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_repayments)
- [ez_votes](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_votes)
- [ez_withdraws](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.aave__ez_withdraws)

### Beacon Chain Tables (ethereum.beacon_chain)
- [fact_blocks](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_blocks)
- [fact_blob_sidecars](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_blob_sidecars)
- [fact_attestations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_attestations)
- [fact_deposits](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_deposits)
- [fact_withdrawals](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_withdrawals)
- [fact_validators](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_validators)
- [fact_validator_balances](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_validator_balances)
- [ez_deposits](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__ez_deposits)
- [ez_withdrawals](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__ez_withdrawals)

### Chainlink Tables (ethereum.chainlink)
- [dim_oracle_feeds](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.chainlink__dim_oracle_feeds)
- [ez_oracle_feeds](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.chainlink__ez_oracle_feeds)
- [fact_oracle_feeds](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.chainlink__fact_oracle_feeds)

### Compound Tables (ethereum.compound)
- [ez_asset_details](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.compound__ez_asset_details)
- [ez_borrows](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.compound__ez_borrows)
- [ez_deposits](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.compound__ez_deposits)
- [ez_liquidations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.compound__ez_liquidations)
- [ez_market_stats](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.compound__ez_market_stats)
- [ez_redemptions](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.compound__ez_redemptions)
- [ez_repayments](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.compound__ez_repayments)

### ENS Tables (ethereum.ens)

- [ez_ens_domains](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.ens__ez_ens_domains)
- [fact_dao_votes](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.ens__fact_dao_votes)
- [fact_registrations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.ens__fact_registrations)
- [fact_renewals](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.ens__fact_renewals)
- [fact_transfers](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.ens__fact_transfers)

### Maker Tables (ethereum.maker)
- [ez_delegations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.maker__ez_delegations)
- [ez_deposits](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.maker__ez_deposits)
- [ez_flash_loans](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.maker__ez_flash_loans)
- [ez_governance_votes](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.maker__ez_governance_votes)
- [ez_liquidations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.maker__ez_liquidations)
- [ez_repayments](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.maker__ez_repayments)
- [ez_vault_creation](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.maker__ez_vault_creation)
- [ez_withdrawals](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.maker__ez_withdrawals)

### Sushi Tables (ethereum.sushi)
- [dim_distributor_reward_schedule](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.sushi__dim_distributor_reward_schedule)
- [ez_borrowing](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.sushi__ez_borrowing)
- [ez_lending](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.sushi__ez_lending)
- [ez_swaps](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.sushi__ez_swaps)

### Synthetix Tables (ethereum.synthetix)
- [ez_snx_staking](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.synthetix__ez_snx_staking)

### Uniswap v3 Tables (ethereum.uniswapv3)
- [ez_lp_actions](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.uniswapv3__ez_lp_actions)
- [ez_pool_stats](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.uniswapv3__ez_pool_stats)
- [ez_pools](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.uniswapv3__ez_pools)
- [ez_position_collected_fees](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.uniswapv3__ez_position_collected_fees)
- [ez_positions](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.uniswapv3__ez_positions)
- [ez_swaps](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.uniswapv3__ez_swaps)

## **Helpful User-Defined Functions (UDFs)**

UDFs are custom functions built by the Flipside team that can be used in your queries to make your life easier. 

Please visit [LiveQuery Functions Overview](https://flipsidecrypto.github.io/livequery-models/#!/overview) for a full list of helpful UDFs.

## **Data Model Overview**

The Ethereum models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of onchain and offchain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Contract Decoding**
### Adding a contract for decoding
To add a contract for decoding, please visit [here](https://science.flipsidecrypto.xyz/abi-requestor/). 

Assuming the submitted ABI is valid, records will be decoded within 24 hours. If records are not decoded within 24 hours, or for any ABI updates, please submit a ticket within our Discord. 

### General Process Overview

The majority of our ABIs have been sourced from Etherscan, and we are constantly asking Etherscan for new ABIs. However, this is not comprehensive, and therefore we must also rely on our users to submit ABIs for decoding.
If we are unable to locate an ABI for the contract from either Etherscan or our users, we will attempt to match the contract to a similar ABI. This is done by comparing the contract bytecode to a list of known contract bytecodes. If we are able to match the contract to a similar ABI, we will decode the contract using the similar ABI. You can see the source of each ABI in the `dim_contract_abis` table within the `abi_source` column. 

Once ABIs have been verified, events within the last day of blocks will be decoded within approximately 90 minutes. Events older than 1 day will be decoded within 24 hours in the majority of cases. The exception here is if the contract has a massive number of events, in which case it may take longer.

## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/earn)
- [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover)
- [Tutorials](https://docs.flipsidecrypto.com/our-data/tutorials)
- [Github](https://github.com/FlipsideCrypto/ethereum-models)
- [Query Editor Shortcuts](https://docs.flipsidecrypto.com/velocity/query-editor-shortcuts)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}
