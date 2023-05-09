-- depends_on: {{ ref('test_silver__transactions_full') }}
{{ missing_txs(ref("test_silver__receipts_full")) }}
