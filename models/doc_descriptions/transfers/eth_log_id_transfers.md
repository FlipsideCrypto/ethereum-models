{% docs eth_log_id_transfers %}

This is the primary key for this table. This is a concatenation of the transaction hash and the event index at which the transfer event occurred. This field can be used to find more details on the event within the ```fact_event_logs``` table.

{% enddocs %}