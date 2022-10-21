{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

 SELECT
    $1 AS SOURCE_TABLE_NAME,
    $2 AS FETCH_TYPE,
    $3 AS SECRET_TYPE,
    $4 as SECRET_SSM_KEY,
    $5 as HOST,
    $6 as ENDPOINT,
    $7 AS PAYLOAD,
    $8 AS CONSUMER_LAMBDA_SIZE,
    $9 AS RATE_LIMIT
FROM VALUES
    (
      'streamline__token_balances_by_date_source',
      'JSON_RPC',
      'HEADER_SECRET',
      'SECRET_FIGMENT_API',
      'www.figment.com',
      '',
      '',
         '{'
      ||   '"jsonrpc": "2.0",'
      ||   '"method": "eth_call"'
      ||   '"params": [{"to": "{CONTRACT_ADDRESS}", "data": "{ADDRESS}"}, false],'
      ||   'id: "{AUTO_ID}"'
      || '}',
      'LARGE',
      16500
    ),
    (
      'streamline__eth_balances_by_date',
      'JSON_RPC',
      'HEADER_SECRET',
      'SECRET_FIGMENT_API',
      'www.figment.com',
      '',
         '{'
      ||   '"jsonrpc": "2.0",'
      ||   '"method": "eth_getBalance",'
      ||   '"params": [{ADDRESS}, {BLOCK_NUMBER_HEX}],'
      ||   'id: "{AUTO_ID}"'
      || '}',
      'LARGE',
      16500
    ),
    (
      'streamline__contract_reads_source',
      'JSON_RPC',
      'HEADER_SECRET',
      'SECRET_FIGMENT_API',
      'www.figment.com',
      '',
         '{'
      ||   '"jsonrpc": "2.0",'
      ||   '"method": "eth_getBalance",'
      ||   '"params": [{"to": "{CONTRACT_ADDRESS}", "data": "{INPUT_DATA}"}, {BLOCK_NUMBER_HEX}],'
      ||   'id: "{ \"CONTRACT_ADDRESS\": \"{CONTRACT_ADDRESS}\", \"FUNCTION_INPUT\": \"{INPUT_DATA}\", \"BLOCK_NUMBER\": \"{BLOCK_NUMBER_HEX}\", \"CALL_NAME\": \"{CALL_NAME}\", \"FUNCTION_INPUT\": \"{INPUT_DATA}\" }"'
      || '}',
      'LARGE',
      16500
    ),
    (
      'streamline__contract_addresses',
      'REST',
      'PARAM_SECRET',
      'SECRET_ETHERSCAN_API',
      'www.etherscan.com',
      '/api?'
      || 'module=contract&'
      || 'actions=getabi&'
      || 'address={ADDRESS}',
      NULL,
      'LARGE',
      16500
    )