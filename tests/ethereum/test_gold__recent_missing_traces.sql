-- depends_on: {{ ref('test_silver__transactions_recent') }}
{{ recent_missing_txs(ref("test_gold__fact_traces_recent")) }}
