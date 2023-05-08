-- depends_on: {{ ref('test_silver__transactions_recent') }}
{{ recent_missing_txs(ref("test_silver__receipts_recent")) }}
