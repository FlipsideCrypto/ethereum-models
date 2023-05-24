-- depends_on: {{ ref('test_silver__blocks_recent') }}
{{ recent_tx_gaps(ref("test_silver__transactions_recent")) }}
