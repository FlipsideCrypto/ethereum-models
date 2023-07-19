-- depends_on: {{ ref('test_silver__blocks_recent') }}
{{ fsc_utils.recent_tx_gaps(ref("test_silver__transactions_recent")) }}
