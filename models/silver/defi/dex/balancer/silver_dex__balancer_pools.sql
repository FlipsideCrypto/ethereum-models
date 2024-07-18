{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    full_refresh = false,
    tags = ['curated']
) }}

WITH pools_registered AS (

    SELECT
        block_number,
        block_timestamp,
        event_index,
        tx_hash,
        contract_address,
        topics [1] :: STRING AS pool_id,
        SUBSTR(
            topics [1] :: STRING,
            1,
            42
        ) AS pool_address,
        _log_id,
        _inserted_timestamp,
        ROW_NUMBER() over (
            ORDER BY
                pool_address
        ) AS row_num
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x3c13bc30b8e878c53fd2a36b679409c073afd75950be43d8858768e956fbc20e' --PoolRegistered
        AND contract_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
tokens_registered AS (
    SELECT
        block_number,
        block_timestamp,
        event_index,
        tx_hash,
        contract_address,
        decoded_flat :poolId :: STRING AS pool_id,
        decoded_flat :tokens AS tokens,
        tokens [0] :: STRING AS token0,
        tokens [1] :: STRING AS token1,
        tokens [2] :: STRING AS token2,
        tokens [3] :: STRING AS token3,
        tokens [4] :: STRING AS token4,
        tokens [5] :: STRING AS token5,
        tokens [6] :: STRING AS token6,
        tokens [7] :: STRING AS token7,
        decoded_flat :assetManagers AS asset_managers,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0xf5847d3f2197b16cdcd2098ec95d0905cd1abdaf415f07bb7cef2bba8ac5dec4' --TokensRegistered
        AND contract_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                pools_registered
        )
        AND tx_status = 'SUCCESS'
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
        pool_address,
        block_number,
        function_sig
    FROM
        pools_registered
        JOIN function_sigs
        ON 1 = 1
),
build_rpc_requests AS (
    SELECT
        pool_address,
        block_number,
        function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': pool_address, 'from': null, 'data': input}, utils.udf_int_to_hex(block_number)],
            concat_ws(
                '-',
                pool_address,
                input,
                block_number
            )
        ) AS rpc_request,
        row_num,
        CEIL(
            row_num / 250
        ) AS batch_no
    FROM
        inputs_pools
        LEFT JOIN pools_registered USING(pool_address)
),
pool_token_reads AS ({% for item in range(10) %}
    (
SELECT
    live.udf_api('POST', CONCAT('{service}', '/', '{Authentication}'),{}, batch_rpc_request, 'Vault/prod/ethereum/quicknode/mainnet') AS read_output, SYSDATE() AS _inserted_timestamp
FROM
    (
SELECT
    ARRAY_AGG(rpc_request) batch_rpc_request
FROM
    build_rpc_requests
WHERE
    batch_no = {{ item }} + 1
    AND batch_no IN (
SELECT
    DISTINCT batch_no
FROM
    build_rpc_requests))) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}),
reads_adjusted AS (
    SELECT
        VALUE :id :: STRING AS read_id,
        VALUE :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        read_id_object [0] :: STRING AS pool_address,
        LEFT(
            read_id_object [1] :: STRING,
            10
        ) AS function_sig,
        read_id_object [2] :: STRING AS block_number,
        _inserted_timestamp
    FROM
        pool_token_reads,
        LATERAL FLATTEN(
            input => read_output :data
        )
),
pool_details AS (
    SELECT
        pool_address,
        function_sig,
        function_name,
        read_result,
        regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_output,
        _inserted_timestamp
    FROM
        reads_adjusted
        LEFT JOIN function_sigs USING(function_sig)
),
FINAL AS (
    SELECT
        pool_address,
        MIN(
            CASE
                WHEN function_name = 'symbol' THEN utils.udf_hex_to_string(
                    segmented_output [2] :: STRING
                )
            END
        ) AS pool_symbol,
        MIN(
            CASE
                WHEN function_name = 'name' THEN utils.udf_hex_to_string(
                    segmented_output [2] :: STRING
                )
            END
        ) AS pool_name,
        MIN(
            CASE
                WHEN read_result :: STRING = '0x' THEN NULL
                ELSE utils.udf_hex_to_int(LEFT(read_result :: STRING, 66))
            END
        ) :: INTEGER AS pool_decimals,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        pool_details
    GROUP BY
        1
)
SELECT
    p.block_number,
    p.block_timestamp,
    p.event_index,
    p.tx_hash,
    p.contract_address,
    p.pool_id,
    f.pool_address,
    f.pool_symbol,
    f.pool_name,
    f.pool_decimals,
    t.token0,
    t.token1,
    t.token2,
    t.token3,
    t.token4,
    t.token5,
    t.token6,
    t.token7,
    t.asset_managers,
    p._log_id,
    f._inserted_timestamp
FROM
    FINAL f
    LEFT JOIN pools_registered p
    ON f.pool_address = p.pool_address
    LEFT JOIN tokens_registered t
    ON p.pool_id = t.pool_id
WHERE
    t.token0 IS NOT NULL
