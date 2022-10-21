{{ config(
    materialized = 'view',
    tags = ['streamline_view'],
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_token_balances(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH layer2s AS (
  SELECT 
    $1 as L2_NAME, 
    $2 as HOST,
    $3 as SECRET_KEY
  FROM VALUES 
    ('arbitrum', 'www.figment.com', 'ARBITRUM_SECRET'), 
    ('avalanche', 'www.figment.com', 'AVALANCHE_SECRET'), 
    ('bsc', 'www.figment.com', 'BSC_SECRET'), 
    ('ethereum', 'www.figment.com', 'ETHEREUM_SECRET'), 
    ('gnosis', 'www.figment.com', 'GNOSIS_SECRET'), 
    ('harmony', 'www.figment.com', 'HARMONY_SECRET'), 
    ('optimism', 'www.figment.com', 'OPTIMISM_SECRET'), 
    ('polygon', 'www.figment.com', 'POLYGON_SECRET')
),
block_height_queues as (
  SELECT
    'table(' || {{this.schema}} || 'evm_block_heights(' || L2_NAME || '))' as SOURCE_TABLE_NAME,
    'evm_block_heights_destination' as DESTINATION_TABLE_NAME,
    HOST,
    SECRET_KEY,
    L2_NAME,
    'block_heights' as UNIT_NAME,
    '{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], id: "{AUTO_ID}"}'
      as PAYLOAD
  FROM layer2s
),
block_queues as (
  SELECT
    'table(' || {{this.schema}} || 'evm_blocks(' || L2_NAME || '))' as SOURCE_TABLE_NAME,
    'evm_blocks_destination' as DESTINATION_TABLE_NAME,
    HOST,
    SECRET_KEY, 
    L2_NAME,
    'blocks' as UNIT_NAME,
    '{"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["{BLOCK_NUMBER}", false], id: "{BLOCK_NUMBER}"}'
      as PAYLOAD,
  FROM layer2s
),
tx_units AS (
    SELECT
        $1 AS UNIT_NAME,
        $2 AS PAYLOAD
    FROM VALUES
        (
          'transactions',
          '{"jsonrpc": "2.0", "method": "eth_getTransactionByHash", "params": ["{TX_HASH}"], id: "{TX_HASH}"}'
        ),
        (
          'receipts',
          '{"jsonrpc": "2.0", "method": "eth_getTransactionReceipt", "params": ["{TX_HASH}"], id: "{TX_HASH}"}'
        ),
        (
          'traces',
          '{"jsonrpc": "2.0", "method": "debug_traceTransaction", "params": ["{TX_HASH}", {"tracer": "callTracer"}], id: "{TX_HASH}"}'
        )
),
tx_queues AS (
  SELECT
    'table(' || {{this.schema}} || '.evm_tx_hashes_source(' || l2.L2_NAME || '))' as SOURCE_TABLE_NAME,
    'evm_tx_' || tu.UNIT_NAME || '_destination' as DESTINATION_TABLE_NAME,
    tu.UNIT_NAME, 
    tu.PAYLOAD, 
    l2.L2_NAME, 
    l2.HOST, 
    l2.SECRET_KEY
  FROM tx_units tu
  CROSS JOIN layer2s l2
),
queues as (
  SELECT * FROM tx_queues
  UNION ALL
  SELECT * FROM block_queues
)
SELECT
    {{this.schema}} || '.' || {{this.identifier}}
      || q.SOURCE_TABLE_NAME as SOURCE_TABLE_NAME,
    SOURCE_TABLE_NAME || UNIT_NAME || '.fifo' as QUEUE_NAME,
    'JSON_RPC' as APPLICATION_LAYER,
    'HEADER_SECRET' as SECRET_TYPE,
    q.SECRET_KEY,
    q.HOST as FETCH_HOST,
    'POST' as FETCH_METHOD,
    NULL as FETCH_ENDPOINT,
    NULL as FETCH_PARAMS,
    q.PAYLOAD as FETCH_PAYLOAD,
    165000 as PRODUCER_BATCH_SIZE,
    16500 as WORKER_BATCH_SIZE,
    5 as WORKER_CONCURRENCY,
    'evm-layer2s' as DESTINATION_BUCKET,
    NULL as BACKOFF_MAX_TIME
FROM queues q