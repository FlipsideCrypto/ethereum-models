{% docs __overview__ %}

# Welcome to the Flipside Crypto Ethereum Models Documentation!

## **What is Flipside?**

[Flipside Crypto](https://flipsidecrypto.xyz/earn) provides Community Enabled Crypto Analytics, allowing our users to create and share data insights on the crypto projects they care most about.

**Flipside Crypto puts pre-modeled and labeled blockchain data in the hands of communities.**

Through dashboard and visualization tools, as well as auto-generated API endpoints, data analysts can easily create queries that answer any question via a tool called [Velocity](https://app.flipsidecrypto.com/velocity?nav=Discover).

**Community members earn bounties for answering questions with data.**

Bounties provide incentive and direction, so crypto projects can quickly source the data insights they need in order to grow.

**Flipside works directly with leading crypto projects to reward on-demand analytics through structured bounty programs.**

Questions sourced directly from the community provide insight into what communities care about as well as analytics needed to drive ecosystem engagement and growth.

## **What does this documentation cover?**
The documentation included here details the design of the Ethereum tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/earn) For more information on how these models are built, please see [the github repository.](https://github.com/FlipsideCrypto/ethereum-models)

### **Quick Links to Table Documentation**

- [dim_contracts](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__dim_contracts)
- [dim_labels](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__dim_labels)
- [fact_blocks](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_blocks)
- [fact_event_logs](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_event_logs)
- [fact_traces](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_traces)
- [fact_hourly_token_prices](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_hourly_token_prices)
- [fact_token_transfers](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_token_transfers)
- [fact_transactions](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__fact_transactions)
- [ez_token_transfers](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_token_transfers)
- [ez_nft_transfers](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_nft_transfers)
- [ez_nft_sales](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_nft_sales)
- [ez_nft_mints](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_nft_mints)
- [ez_dez_swaps](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_dex_swaps)
- [ez_balance_deltas](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_balance_deltas)
- [ez_current_balances](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.core__ez_current_balances)
- [beacon_chain.fact_blocks](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_blocks)
- [beacon_chain.fact_attestations](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_attestations)
- [beacon_chain.fact_deposits](https://flipsidecrypto.github.io/ethereum-models/#!/model/model.ethereum_models.beacon_chain__fact_deposits)



## **Data Model Overview**

The Ethereum models are built a few different ways, but the core fact table are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available in Velocity

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. 

A user-defined-function (UDF) is available to decode hex encoded values to integers in this database. You can call this UDF by using `ethereum.public.udf_hex_to_int(FIELD::string)`.

## **Contract Decoding**
### Adding a contract for decoding
To add a contract for decoding, please visit [here](https://science.flipsidecrypto.xyz/abi-requestor/). 

Assuming the submitted ABI is valid, records will be decoded within 24 hours. If records are not decoded with 24 hours, or for any ABI updates, please submit a ticket within our Discord. 

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
