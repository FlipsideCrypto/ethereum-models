-- depends_on: {{ ref('test_silver__blocks_full') }}
{{ tx_gaps(ref("test_silver__transactions_full")) }}
