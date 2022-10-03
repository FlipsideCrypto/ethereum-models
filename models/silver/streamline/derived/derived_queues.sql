{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

 SELECT
    $1 AS QUEUE_NAME,
    $2 AS SQL_STATEMENT,
    $3 AS FETCH_TYPE,
    $4 AS SECRET_TYPE,
    $5 as SECRET_SSM_KEY,
    $6 as HOST,
    $7 as ENDPOINT,
    $8 AS PAYLOAD,
    $9 AS PRODUCER_LAMBDA_SIZE,
    $10 AS CONSUMER_LAMBDA_SIZE,
    $11 AS BATCH_SIZE,
    $12 as MAX_RETRIES,
    $13 AS SHARD_COUNT,
    $14 AS MAX_CONCURRENT_BATCHES_PER_SHARD
FROM VALUES
    (
      'token_balances_by_date',
      'SELECT '
      ||   'CONTRACT_ADDRESS,'
      ||   'BLOCK_NUMBER,'
      ||   '\'0x70a08231000000000000000000000000\' || LTRIM(ADDRESS, 2) as ADDRESS'
      || ' FROM '
      ||   'streamline.token_balances_by_date'
      || ' ORDER BY '
      ||   '_INSERTED_TIMESTAMP ASC, ID ASC',
      'JSON_RPC',
      'HEADER_SECRET',
      'SECRET_FIGMENT_API',
      'www.figment.com',
      '',
         '{'
      ||   '"jsonrpc": "2.0",'
      ||   '"method": "eth_call"'
      ||   '"params": [{"to": "{CONTRACT_ADDRESS}", "data": "{ADDRESS}"}, false],'
      ||   'id: "{ \"CONTRACT_ADDRESS\": \"{CONTRACT_ADDRESS}\", \"ADDRESS\": \"{ADDRESS}\", \"BLOCK_NUMBER\": \"{BLOCK_NUMBER}\" }"'
      || '}',
      'SMALL', 'LARGE', 16500, 20, 2, 2
    ),
    (
      'eth_balances_by_date',
      'SELECT '
      || 'ADDRESS,'
      || 'hex_encode(BLOCK_NUMBER) as BLOCK_NUMBER_HEX'
      || ' FROM '
      || 'streamline.eth_balances_by_date'
      || ' ORDER BY '
      || '_INSERTED_TIMESTAMP ASC, ID ASC',
      'JSON_RPC',
      'HEADER_SECRET',
      'SECRET_FIGMENT_API',
      'www.figment.com',
      '',
         '{'
      ||   '"jsonrpc": "2.0",'
      ||   '"method": "eth_getBalance",'
      ||   '"params": [{ADDRESS}, {BLOCK_NUMBER_HEX}],'
      ||   'id: "{ \"ADDRESS\": \"{ADDRESS}\", \"BLOCK_NUMBER\": \"{BLOCK_NUMBER}\" }"'
      || '}',
      'SMALL', 'LARGE', 16500, 20, 2, 2
    ),
    (
      'contract_reads',
      'SELECT '
      || 'CONTRACT_ADDRESS,'
      || 'CASE ('
      ||   'WHEN FUNCTION_INPUT IS NULL THEN FUNCTION_SIGNATUE'
      ||   'WHEN startswith(FUNCTION_INPUT, \'0x\') THEN LTRIM(FUNCTION_INPUT, 2)'
      ||   'ELSE FUNCTION_INPUT'
      || ') AS INPUT_DATA,'
      || 'CALL_NAME,'
      || 'IFF(typeof(BLOCK_NUMBER) = \'STRING\', BLOCK_NUMBER, hex_encode(BLOCK_NUMBER)) as BLOCK_NUMBER_HEX'
      || ' FROM '
      ||   'streamline.contract_reads'
      || ' ORDER BY '
      ||   '_INSERTED_TIMESTAMP ASC, ID ASC',
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
      'SMALL', 'LARGE', 16500, 20, 2, 2
    ),
    (
      'contract_abis',
      'SELECT '
      || 'ADDRESS,'
      || 'BLOCK_NUMBER'
      || ' FROM '
      || 'streamline.contract_addresses'
      || ' ORDER BY '
      || '_INSERTED_TIMESTAMP ASC, ID ASC',
      'REST',
      'PARAM_SECRET',
      'SECRET_ETHERSCAN_API',
      'www.etherscan.com',
      '/api?'
      || 'module=contract&'
      || 'actions=getabi&'
      || 'address={ADDRESS}',
      NULL,
      'SMALL', 'LARGE', 16500, 20, 2, 2
    )