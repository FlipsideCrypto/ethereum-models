{% docs evm_bridge_table_doc %}

A convenience table that aggregates bridge activity from event_logs, traces and transfers, including bridge deposits and transfers sent from the following protocols: ACROSS, ALLBRIDGE, AXELAR, CELER, CBRIDGE, HOP, MESON, MULTICHAIN, NATIVE, STARGATE, SYMBIOSIS, SYNAPSE, WORMHOLE along with other helpful columns, including an amount USD where available. Note, this table only includes records for the protocols listed above with live, onchain bridge activity and may not represent the complete bridging picture.

{% enddocs %}

{% docs evm_bridge_platform %}

The platform or protocol from which the bridge transaction or event originates.

{% enddocs %}

{% docs evm_bridge_origin_from %}

The from address where the transaction originated from. This may be an EOA or contract address, however in most cases this is the user that initiated the bridge deposit or transfer.

{% enddocs %}

{% docs evm_bridge_sender %}

The address that initiated the bridge deposit or transfer. This address is the sender of the tokens/assets being bridged to the destination chain. This may be an EOA or contract address.

{% enddocs %}

{% docs evm_bridge_receiver %}

The designated address set to receive the bridged tokens on the target chain after the completion of the bridge transaction. This may be an EOA or contract address.

{% enddocs %}

{% docs evm_bridge_destination_chain %}

The name of the blockchain network to which the assets are being bridged. It could be any EVM compatible chain or other blockchain networks that the bridging protocol supports.

{% enddocs %}

{% docs evm_bridge_destination_chain_id %}

The numeric identifier associated with the destination blockchain network. This is specific to the chain and helps in uniquely identifying it.

{% enddocs %}

{% docs evm_bridge_address %}

The address of the contract responsible for handling the bridge deposit or transfer. This contract mediates the transfer and ensures that assets are sent and received appropriately.

{% enddocs %}

{% docs evm_bridge_token_address %}

The address associated with the token that is being bridged. It provides a unique identifier for the token within its origin blockchain.

{% enddocs %}

{% docs evm_bridge_token_symbol %}

The symbol representing the token being bridged. This provides a shorthand representation of the token.

{% enddocs %}

{% docs evm_bridge_amount_unadj %}

The raw, non-decimal adjusted amount of tokens involved in the bridge transaction.

{% enddocs %}

{% docs evm_bridge_amount %}

The decimal adjusted amount of tokens involved in the bridge transaction, where available.

{% enddocs %}

{% docs evm_bridge_amount_usd %}

The value of the bridged tokens in USD at the time of the bridge transaction, where available.

{% enddocs %}