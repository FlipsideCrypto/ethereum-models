{% docs evm_logs_table_doc %}

This table contains flattened event logs from transactions on this EVM blockchain. Transactions may have multiple events, which are denoted by the event index for a transaction hash. Therefore, this table is unique on the combination of transaction hash and event index. Please see `fact_decoded_event_logs` or `ez_decoded_event_logs` for the decoded event logs.

{% enddocs %}


{% docs evm_event_index %}

Event number within a transaction.

{% enddocs %}


{% docs evm_event_inputs %}

The decoded event inputs for a given event.

{% enddocs %}

{% docs evm_event_removed %}

Whether the event has been removed from the transaction.

{% enddocs %}


{% docs evm_log_id_events %}

This is the primary key for this table. This is a concatenation of the transaction hash and the event index at which the event occurred. This field can be used within other event based tables such as ```fact_transfers``` & ```ez_token_transfers```.

{% enddocs %}


{% docs evm_logs_contract_address %}

The address interacted with for a given event.

{% enddocs %}


{% docs evm_logs_contract_name %}

The name of the contract or token, where possible.

{% enddocs %}


{% docs evm_logs_data %}

The un-decoded event data.

{% enddocs %}


{% docs evm_logs_tx_hash %}

Transaction hash is a unique 66-character identifier that is generated when a transaction is executed. This field will not be unique in this table, as a given transaction can include multiple events.

{% enddocs %}


{% docs evm_topics %}

The un-decoded event input topics.

{% enddocs %}


{% docs evm_topic_0 %}

The first topic of the event, which is a unique identifier for the event.

{% enddocs %}


{% docs evm_topic_1 %}  

The second topic of the event, if applicable.

{% enddocs %}


{% docs evm_topic_2 %}

The third topic of the event, if applicable.

{% enddocs %}


{% docs evm_topic_3 %}

The fourth topic of the event, if applicable.  

{% enddocs %}