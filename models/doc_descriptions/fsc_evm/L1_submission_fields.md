{% docs evm_batch_size %}

Total transactions included within the batch on the OP stack L2 chain.

{% enddocs %}

{% docs evm_max_block %}

The max block on the OP stack L2 chain this batch relates to.

{% enddocs %}

{% docs evm_min_block %}

The min block on the OP stack L2 chain this batch relates to.

{% enddocs %}

{% docs evm_batch_root %}

Root of the batch, either for submission or state on the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_block_no %}

The Ethereum block number that contained the batch from the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_block_time %}

The timestamp of the Ethereum block that contained this batch from the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_fee %}

The L1 portion of fees paid.

{% enddocs %}

{% docs evm_l1_fee_scalar %}

This value covers the change in L1 gas price between the time the transaction is submitted and when it is published from the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_gas_price %}

The gas price for L1 transactions when the transaction was processed from the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_gas_used %}

The gas used on L1 to publish the transaction from the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_state_batch %}

The batch index of when this block was included in the Ethereum state root. This column will be deprecated 8/7 and will be consolidated into an array column consisting of all L1 submission details from the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_state_tx_hash %}

The L1 tx hash of when this block was included in the Ethereum state root. This column will be deprecated 8/7 and will be consolidated into an array column consisting of all L1 submission details from the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_sub_batch %}

The batch index of when this block was submitted to L1. This column will be deprecated 8/7 and will be consolidated into an array column consisting of all L1 submission details from the OP stack L2 chain.

{% enddocs %}

{% docs evm_l1_sub_tx_hash %}

The L1 tx hash of when this block was submitted to L1. This column will be deprecated 8/7 and will be consolidated into an array column consisting of all L1 submission details from the OP stack L2 chain.

{% enddocs %}

{% docs evm_prev_total_elements %}

Confirmed blocks prior to this batch on the OP stack L2 chain.

{% enddocs %}

