{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

 SELECT
    {{this.schema}} || '.' || {{this.identifier}} || '.' 
        || $1 AS SOURCE_TABLE_NAME,
    SOURCE_TABLE_NAME || '_destination' as DESTINATION_TABLE_NAME,
    SOURCE_TABLE_NAME || '.fifo' as QUEUE_NAME,
    $2 AS APPLICATION_LAYER,
    $3 AS SECRET_TYPE,
    $4 as SECRET_KEY,
    $5 as FETCH_HOST,
    $6 as FETCH_METHOD,
    $7 as FETCH_ENDPOINT,
    $8 as FETCH_PARAMS,
    $9 AS FETCH_PAYLOAD,
    165000 AS PRODUCER_BATCH_SIZE,
    16500 AS WORKER_BATCH_SIZE,
    5 AS WORKER_CONCURRENCY,
    'ethereum-models' AS DESTINATION_BUCKET,
    NULL AS BACKOFF_MAX_TIME
FROM VALUES
    (
      'streamline__token_balances_by_date_source',
      'JSON_RPC',
      'HEADER_SECRET',
      'SECRET_FIGMENT_API',
      'www.figment.com',
      'POST'
      NULL,
      NULL,
         '{'
      ||   '"jsonrpc": "2.0",'
      ||   '"method": "eth_call"'
      ||   '"params": [{"to": "{CONTRACT_ADDRESS}", "data": "{ADDRESS}"}, false],'
      ||   'id: "{AUTO_ID}"'
      || '}'
    ),
    (
      'streamline__eth_balances_by_date',
      'JSON_RPC',
      'HEADER_SECRET',
      'SECRET_FIGMENT_API',
      'www.figment.com',
      'POST',
      NULL,
      NULL,
         '{'
      ||   '"jsonrpc": "2.0",'
      ||   '"method": "eth_getBalance",'
      ||   '"params": [{ADDRESS}, {BLOCK_NUMBER_HEX}],'
      ||   'id: "{AUTO_ID}"'
      || '}'
    ),
    (
      'streamline__contract_reads_source',
      'JSON_RPC',
      'HEADER_SECRET',
      'SECRET_FIGMENT_API',
      'www.figment.com',
      'POST',
      NULL,
      NULL,
         '{'
      ||   '"jsonrpc": "2.0",'
      ||   '"method": "eth_getBalance",'
      ||   '"params": [{"to": "{CONTRACT_ADDRESS}", "data": "{INPUT_DATA}"}, {BLOCK_NUMBER_HEX}],'
      ||   'id: "{AUTO_ID}"'
      || '}'
    ),
    (
      'streamline__contract_addresses',
      'REST',
      'PARAM_SECRET',
      'SECRET_ETHERSCAN_API',
      'www.etherscan.com',
      'GET',
      '/api',
      PARSE_JSON(''),
      NULL
    )