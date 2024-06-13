{% docs olas_dim_registry_metadata_table_doc %}

This table contains dimensional metadata for the registry contracts, including details about the various Agent, Component and Service entities registered in the OLAS ecosystem. The metadata is sourced via contract reads on the tokenURI function, and typically direct to IPFS.

{% enddocs %}

{% docs olas_ez_service_registrations_table_doc %}

This convenience table contains fact-based records of service registrations within the OLAS protocol, capturing essential information about each registered service event and includes a join on the `dim_registry_metadata` table for additional details pertaining to each service_id, such as name and description.

{% enddocs %}

{% docs olas_fact_service_event_logs_table_doc %}

This fact-based table contains all emitted event logs related to registered services and service multisigs within the OLAS protocol. This is accomplished by joining all events where the transaction's `origin_to_address` = `multisig_address` to showcase the onchain interactions with each service.

{% enddocs %}

{% docs olas_ez_olas_bonding_table_doc %}

This convenience table tracks OLAS token bonding events and the amount of OLAS bonded, in addition to other related details. The Autonolas Tokenomics bonding mechanism enables users holding supported LP-pair assets to deposit them into the Autonolas depository smart contract. In return, they receive OLAS tokens at a discount relative to the price quoted on the relevant DEX after a vesting time. For more information, please visit Olas' [Official Documentation](https://docs.autonolas.network/protocol/tokenomics/#incentivizing-software-developers).

{% enddocs %}

{% docs olas_ez_olas_locking_table_doc %}

This convenience table captures the locking of OLAS tokens for both Voting Escrow OLAS (veOLAS) and Burnable Locked OLAS (buOLAS), including information on the amount locked and the duration of the lock-up periods.

{% enddocs %}

{% docs olas_ez_service_donations_table_doc %}

This convenience table records ETH donations made to services within the OLAS ecosystem, including the donor, recipient service, and amount donated, denominated in ETH and USD.

{% enddocs %}

{% docs olas_ez_unit_registrations_table_doc %}

This convenience table contains fact-based records of unit registrations (Agents and Components) within the OLAS protocol, capturing essential information about each registered unit event and includes a join on the `dim_registry_metadata` table for additional details pertaining to each unit_id, such as name and description.

{% enddocs %}

{% docs olas_fact_pol_transfers_table_doc %}

This fact-based table tracks Protocol Owned Liquidity (POL) transfers, recording the movement of LP tokens owned by the OLAS protocol (Treasury: `0xa0da53447c0f6c4987964d8463da7e6628b30f82`, GnosisSafe Proxy: `0x87cc0d34f6111c8a7a4bdf758a9a715a3675f941`). For more information on the LP tokens being tracked, please visit Olas' [Bonding Documentation](https://bond.olas.network/).

{% enddocs %}

{% docs olas_ez_olas_staking_table_doc %}

This fact-based convenience table contains OLAS token staking events, including deposits (Stake), withdrawals (Unstake), and amount USD where available.

{% enddocs %}

{% docs olas_ez_service_staking_table_doc %}

This fact-based convenience table contains Service staking events, including epoch, owner and multisig addresses, the relevant staking program, and an additional join on the `dim_registry_metadata` table for details pertaining to each service_id, such as name and description.

{% enddocs %}

{% docs olas_fact_mech_activity_table_doc %}

This fact-based table contains all mech requests (prompts) and delivered data, including offchain metadata links to view the requests/responses to/from AI models. For more information, please visit Olas' [Official Mech Documentation](https://docs.autonolas.network/product/mechkit/).

{% enddocs %}

{% docs olas_ez_mech_activity_table_doc %}

This convenience table consolidates mech request and delivery activities within the OLAS protocol, combining request and delivery data (`prompt` and `deliver` links) into a unified set of records, emulating the format of the [AIMechs app](https://aimechs.autonolas.network/mech). 

{% enddocs %}

{% docs olas_ez_service_checkpoints_table_doc %}

This fact-based convenience table contains decoded service Checkpoint events from various applicable staking contracts and programs, and includes a join on the `dim_registry_metadata` table for additional details pertaining to each service_id, such as name and description.

{% enddocs %}

{% docs olas_ez_service_evictions_table_doc %}

This fact-based convenience table contains decoded ServiceEvicted events on various applicable staking contracts and programs, and includes a join on the `dim_registry_metadata` table for additional details pertaining to each service_id, such as name and description.

{% enddocs %}
