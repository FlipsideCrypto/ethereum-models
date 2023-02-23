{{ config(
    materialized = 'incremental',
    unique_key = "pool_id",
    full_refresh = false
) }}

WITH contract_deployments AS (

SELECT
    tx_hash AS tx_hash,
    block_number AS block_number,
    block_timestamp AS block_timestamp,
    from_address AS deployer_address,
    to_address AS contract_address,
    _inserted_timestamp AS _inserted_timestamp,
    ROW_NUMBER() OVER (ORDER BY contract_address) AS row_num
FROM
    {{ ref('silver__traces' )}}
WHERE 
    -- curve contract deployers
    from_address IN (
        '0x7eeac6cddbd1d0b8af061742d41877d7f707289a',
        '0xf18056bbd320e96a48e3fbf8bc061322531aac99',
        '0xc447fcaf1def19a583f97b3620627bf69c05b5fb',
        '0xb9fc157394af804a3578134a6585c0dc9cc990d4',
        '0xfd6f33a0509ec67defc500755322abd9df1bd5b8',
        '0xbf7d65d769e82e7b862df338223263ba33f72623',
        '0xa6df4fcb1ca559155a678e9aff5de3f210c0ff84',
        '0x0959158b6040d32d04c301a72cbfd6b39e21c9ae',
        '0x745748bcfd8f9c2de519a71d789be8a63dd7d66c',
        '0x3e0139ce3533a42a7d342841aee69ab2bfee1d51',
        '0xbabe61887f1de2713c6f97e567623453d3c79f67',
        '0x7f7abe23fc1ad4884b726229ceaafb1179e9c9cf'
    )
    AND TYPE ilike 'create%'
    AND TX_STATUS ilike 'success'
{% if is_incremental() %}
AND to_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
QUALIFY(ROW_NUMBER() OVER(PARTITION BY to_address ORDER BY block_timestamp ASC)) = 1
),

function_sigs AS (

SELECT
    '0xc6610657' AS function_sig, 
    'coins' AS function_name
UNION ALL
SELECT
    '0x06fdde03' AS function_sig, 
    'name' AS function_name
UNION ALL
SELECT
    '0x95d89b41' AS function_sig, 
    'symbol' AS function_name
UNION ALL
SELECT
    '0x313ce567' AS function_sig, 
    'decimals' AS function_name
),

function_inputs AS (
    SELECT
        SEQ4() AS function_input
    FROM
        TABLE(GENERATOR(rowcount => 8))
),

inputs_coins AS (

SELECT
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    (ROW_NUMBER() OVER (PARTITION BY contract_address
        ORDER BY block_number)) - 1 AS function_input
FROM contract_deployments
JOIN function_sigs ON 1=1
JOIN function_inputs ON 1=1
    WHERE function_name = 'coins'
),

inputs_pool_details AS (

SELECT
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    NULL AS function_input
FROM contract_deployments
JOIN function_sigs ON 1=1
WHERE function_name IN ('name','symbol','decimals')
),

all_inputs AS (

SELECT
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    function_input
FROM inputs_coins
UNION ALL
SELECT
    deployer_address,
    contract_address,
    block_number,
    function_sig,
    function_input
FROM inputs_pool_details
),

pool_token_reads AS (

{% for item in range(5) %}
(
SELECT
    ethereum.streamline.udf_json_rpc_read_calls(
        node_url,
        headers,
        PARSE_JSON(batch_read)
    ) AS read_output,
    SYSDATE() AS _inserted_timestamp
FROM (
    SELECT
        CONCAT('[', LISTAGG(read_input, ','), ']') AS batch_read
    FROM (
        SELECT 
            deployer_address,
            contract_address,
            block_number,
            function_sig,
            function_input,
            CONCAT(
                '[\'',
                contract_address,
                '\',',
                block_number,
                ',\'',
                function_sig,
                '\',\'',
                (CASE WHEN function_input IS NULL THEN '' ELSE function_input::STRING END),
                '\']'
                ) AS read_input,
                row_num
        FROM all_inputs
        LEFT JOIN contract_deployments USING(contract_address) 
            ) ready_reads_pools
    WHERE row_num BETWEEN {{ item * 500 + 1 }} AND {{ (item + 1) * 500}}
    ) batch_reads_pools
JOIN streamline.crosschain.node_mapping ON 1=1 
    AND chain = 'ethereum'
) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
),

reads_adjusted AS (
    
SELECT
        VALUE :id :: STRING AS read_id,
        VALUE :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        read_id_object [0] :: STRING AS contract_address,
        read_id_object [1] :: STRING AS block_number,
        read_id_object [2] :: STRING AS function_sig,
        read_id_object [3] :: STRING AS function_input,
        _inserted_timestamp
FROM
    pool_token_reads,
    LATERAL FLATTEN(
        input => read_output [0] :data
    ) 
),

tokens AS (
    
SELECT
    contract_address,
    function_sig,
    function_name,
    read_result,
    regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}')[0]AS segmented_token_address,
    _inserted_timestamp
FROM reads_adjusted
LEFT JOIN function_sigs USING(function_sig)
WHERE read_result IS NOT NULL
    AND function_name = 'coins'
),

pool_details AS (

SELECT
    contract_address,
    function_sig,
    function_name,
    read_result,
    regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_output,
    _inserted_timestamp
FROM reads_adjusted
LEFT JOIN function_sigs USING(function_sig)
WHERE read_result IS NOT NULL
    AND function_name IN ('name','symbol','decimals')
),

FINAL AS (
SELECT
    t.contract_address AS pool_address,
    CONCAT('0x',SUBSTRING(t.segmented_token_address,25,40)) AS token_address,
    MIN(CASE WHEN p.function_name = 'symbol' THEN TRY_HEX_DECODE_STRING(RTRIM(p.segmented_output [2] :: STRING, 0)) END) AS pool_symbol,
    MIN(CASE WHEN p.function_name = 'name' THEN CONCAT(TRY_HEX_DECODE_STRING(p.segmented_output [2] :: STRING),
        TRY_HEX_DECODE_STRING(segmented_output [3] :: STRING)) END) AS pool_name,
    MIN(CASE 
            WHEN p.read_result::STRING = '0x' THEN NULL
            ELSE ethereum.public.udf_hex_to_int(LEFT(p.read_result::STRING,66))
        END)::INTEGER  AS pool_decimals,
    CONCAT(
        t.contract_address,
        '-',
        CONCAT('0x',SUBSTRING(t.segmented_token_address,25,40))
    ) AS pool_id,
    MAX(t._inserted_timestamp) AS _inserted_timestamp
FROM tokens t
LEFT JOIN pool_details p ON t.contract_address = p.contract_address
WHERE token_address IS NOT NULL 
    AND token_address <> '0x0000000000000000000000000000000000000000'
GROUP BY 1,2
)

SELECT
    pool_address,
    CASE
        WHEN token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE token_address
    END AS token_address,
    CASE 
        WHEN token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 'WETH'
        WHEN pool_symbol IS NULL THEN c.symbol
        ELSE pool_symbol
    END AS pool_symbol,
    pool_name,
    CASE 
        WHEN token_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '18'
    	WHEN pool_decimals IS NULL THEN c.decimals
        ELSE pool_decimals
    END AS pool_decimals,
    pool_id,
    f._inserted_timestamp
FROM FINAL f
LEFT JOIN {{ ref('silver__contracts') }} c ON f.token_address = c.address