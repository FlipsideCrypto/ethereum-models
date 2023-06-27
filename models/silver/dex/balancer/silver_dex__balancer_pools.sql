{{ config(
    materialized = 'incremental',
    unique_key = "pool_address",
    full_refresh = false
) }}

WITH pool_creation AS (

    SELECT
        tx_hash,
        topics [1] :: STRING AS pool_id,
        SUBSTR(topics [1] :: STRING,1,42) AS contract_address,
        block_number,
        _inserted_timestamp,
        ROW_NUMBER() OVER (ORDER BY contract_address) AS row_num
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics[0]::STRING = '0x3c13bc30b8e878c53fd2a36b679409c073afd75950be43d8858768e956fbc20e'
        AND contract_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
{% if is_incremental() %}
AND contract_address NOT IN (
        SELECT
            DISTINCT pool_address
        FROM
            {{ this }}
    )
{% endif %}

),

function_sigs AS (

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

inputs_pools AS (

SELECT
    contract_address,
    block_number,
    function_sig,
    (ROW_NUMBER() OVER (PARTITION BY contract_address
        ORDER BY block_number)) - 1 AS function_input
FROM pool_creation
JOIN function_sigs ON 1=1 
),

pool_token_reads AS (

{% for item in range(50) %}
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
                function_input,
                '\']'
                ) AS read_input,
                row_num
        FROM inputs_pools
        LEFT JOIN pool_creation USING(contract_address) 
            ) ready_reads_pools
    WHERE row_num BETWEEN {{ item * 50 + 1 }} AND {{ (item + 1) * 50}}
    ) batch_reads_pools
JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }} ON 1=1 
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

pool_details AS (

SELECT
    contract_address AS pool_address,
    function_sig,
    function_name,
    read_result,
    regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_output,
    _inserted_timestamp
FROM reads_adjusted
LEFT JOIN function_sigs USING(function_sig)
    ),

FINAL AS (
SELECT
    pool_address,
    MIN(CASE WHEN function_name = 'symbol' THEN TRY_HEX_DECODE_STRING(segmented_output [2] :: STRING) END) AS pool_symbol,
    MIN(CASE WHEN function_name = 'name' THEN TRY_HEX_DECODE_STRING(segmented_output [2] :: STRING) END) AS pool_name,
    MIN(CASE 
            WHEN read_result::STRING = '0x' THEN NULL
            ELSE utils.udf_hex_to_int(LEFT(read_result::STRING,66))
        END)::INTEGER  AS pool_decimals,
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM pool_details
GROUP BY 1
)

SELECT
    pool_address,
    pool_symbol,
    pool_name,
    pool_decimals,
    _inserted_timestamp
FROM FINAL 
WHERE pool_name IS NOT NULL 
    AND pool_symbol IS NOT NULL