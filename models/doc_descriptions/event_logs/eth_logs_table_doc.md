{% docs eth_logs_table_doc %}

[BETA TABLE] - This table contains flattened event logs from transactions on the Ethereum Blockchain. Transactions may have multiple events, which are denoted by the event index for a transaction hash. Therefore, this table is unique on the combination of transaction hash and event index. Event names are decoded in this table where possible. The event inputs column will contain the log details in JSON format. Specific fields can be pulled from this column using the following sample format: ```event_inputs:<FILED_NAME>::<DATA TYPE> as <FIELD_NAME>```. 

{% enddocs %}