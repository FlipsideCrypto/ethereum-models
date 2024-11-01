{% docs evm_internal_column %}

Deprecated. This column is no longer used. Please remove from your query by Jan. 10 2024.

{% enddocs %}

{% docs evm_amount_deprecation %}

This column is being deprecated for standardization purposes on Jan. 10 2024. Please use the equivalent column without the native asset prefix. For example, use `amount` instead of `avax_amount`.

{% enddocs %}

{% docs evm_block_number %}

Also known as block height. The block number, which indicates the length of the blockchain, increases after the addition of each new block.   

{% enddocs %}

{% docs evm_block_timestamp %}

The date and time at which the block was produced.  

{% enddocs %}

{% docs evm_from_address %}

The sending address of this transaction. 

{% enddocs %}

{% docs evm_ingested_at %}

Internal column.

{% enddocs %}

{% docs evm_to_address %}

The receiving address of this transaction. This can be a contract address. 

{% enddocs %}

{% docs evm_pk %}

The unique identifier for each row in the table.

{% enddocs %}

{% docs evm_inserted_timestamp %}

The UTC timestamp at which the row was inserted into the table.

{% enddocs %}

{% docs evm_modified_timestamp %}

The UTC timestamp at which the row was last modified.

{% enddocs %}

{% docs evm_precise_amount_unadjusted %}

The precise, unadjusted amount of the transaction. This is returned as a string to avoid precision loss. 

{% enddocs %}

{% docs evm_precise_amount_adjusted %}

The precise, adjusted amount of the transaction. This is returned as a string to avoid precision loss. 

{% enddocs %}

{% docs evm_value_hex %}

The value of the transaction in hexadecimal format.

{% enddocs %}

{% docs evm_column_deprecation_notice %}

This column is being deprecated for standardization purposes.

{% enddocs %}

{% docs evm_table_deprecation_notice %}

This table is being deprecated for standardization purposes.

{% enddocs %}
