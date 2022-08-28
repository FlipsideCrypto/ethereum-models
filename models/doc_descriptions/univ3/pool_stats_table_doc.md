{% docs pool_stats_table_doc %}

Statistics for each pool, appened each time a transaction triggers a Pool Event (i.e. 'Initialize', 'Mint', 'Collect', 'Burn', 'Swap', 'Flash', 'IncreaseObservationCardinalityNext', 'SetFeeProtocol', 'CollectProtocol', etc.). 

A new record is appended each time this occurs. These stats are read from the Pool contract state leveraging Flipside's fully archival Ethereum cluster.

{% enddocs %}