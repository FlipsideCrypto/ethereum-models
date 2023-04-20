{% docs deprecation %}

Deprecating soon: Please migrate queries to `fact_decoded_event_logs`, `ez_decoded_event_logs` or use manual parsing of topics and data. The following columns will be deprecated on 4/3/23:

`Fact_event_logs` Columns:
- `event_name`
- `event_inputs`
- `contract_name`

`Fact_transactions` Columns:
- `tx_json`
{% enddocs %}

