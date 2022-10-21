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
    L2_NAME, 
    HOST,
    SECRET_SSM_KEY
  FROM ref('evm_layer2s')
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
    tu.UNIT_NAME, tu.PAYLOAD, l2.L2_NAME, l2.HOST, l2.SECRET_SSM_KEY,
    '{{this.schema}}.{{this.identifier}}' 
      || 'table(evm_tx_hashes(' || l2.L2_NAME || '))' as SOURCE_TABLE_NAME
  FROM tx_units tu
  CROSS JOIN layer2s l2
)
block_queues as (
  SELECT
    HOST, SECRET_SSM_KEY, L2_NAME,
    'blocks' as UNIT_NAME,
    '{{this.schema}}.{{this.identifier}}' 
      || 'table(evm_blocks(' || L2_NAME || '))' as SOURCE_TABLE_NAME,
    '{"jsonrpc": "2.0", "method": "eth_getBlockByNumber", "params": ["{BLOCK_NUMBER}", false], id: "{BLOCK_NUMBER}"}'
      as PAYLOAD
  FROM layer2s
),
queues as (
  SELECT * FROM tx_queues
  UNION ALL
  SELECT * FROM block_queues
)
SELECT
    PAYLOAD, HOST, SECRET_SSM_KEY,
    L2_NAME || '_' || UNIT_NAME as QUEUE_NAME,
    'JSON_RPC' as APPLICATION_LAYER,
    'HEADER_SECRET' as SECRET_TYPE,
    'SMALL' as CONSUMER_LAMBDA_SIZE,
    165000 as PRODUCER_BATCH_SIZE,
    16500 as WORKER_BATCH_SIZE
FROM queues