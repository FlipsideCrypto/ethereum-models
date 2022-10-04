{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

WITH units AS (
    SELECT
        $1 AS NAME,
        $2 AS PARTIAL_STATEMENT,
        $3 AS PAYLOAD,
        $4 AS PRODUCER_LAMBDA_SIZE,
        $5 AS CONSUMER_LAMBDA_SIZE,
        $6 AS BATCH_SIZE,
        $7 AS SHARD_COUNT,
        $8 AS MAX_CONCURRENT_BATCHES_PER_SHARD
    FROM VALUES
        (
          'blocks',
          'SELECT * FROM evm_blocks WHERE LAYER2 = ',
          '{"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["{BLOCK_NUMBER}", false], id: "{BLOCK_NUMBER}"}',
          'SMALL', 'SMALL', 16500, 2, 2
        ), 
        (
          'transactions',
          'SELECT * FROM evm_tx_hashes WHERE LAYER2 = ',
          '{"jsonrpc": "2.0", "method": "eth_getTransactionByHash", "params": ["{TX_HASH}"], id: "{TX_HASH}"}',
           'SMALL', 'SMALL', 16500, 2, 2
        ), 
        (
          'receipts',
          'SELECT * FROM evm_tx_hashes WHERE LAYER2 = ',
          '{"jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": ["{TX_HASH}"], id: "{TX_HASH}"}',
          'SMALL', 'SMALL', 16500, 2, 2
        ), 
        (
          'traces',
          'SELECT * FROM evm_tx_hashes WHERE LAYER2 = ',
          '{"jsonrpc": "2.0", "method": "debug_traceTransaction", "params": ["{TX_HASH}", {"tracer": "callTracer"}], id: "{TX_HASH}"}',
          'SMALL', 'SMALL', 16500, 2, 2
        )
)
SELECT
    l2.NAME || '_' || u.NAME as QUEUE_NAME,
    u.PARTIAL_STATEMENT || l2.NAME || ' ORDER BY _INSERTED_TIMESTAMP ASC, ID ASC' as SQL_STATEMENT,
    'JSON_RPC' as FETCH_TYPE,
    'HEADER_SECRET' as SECRET_TYPE,
    l2.SECRET_SSM_KEY,
    L2.HOST,
    l2.ENDPOINT,
    u.PAYLOAD,
    u.PRODUCER_LAMBDA_SIZE,
    u.CONSUMER_LAMBDA_SIZE,
    u.BATCH_SIZE,
    20 as MAX_RETRIES,
    u.SHARD_COUNT,
    u.MAX_CONCURRENT_BATCHES_PER_SHARD
FROM
    units u
CROSS JOIN
    {{ ref('evm_layer2s') }} l2