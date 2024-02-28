{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    full_refresh = false,
    tags = ['non_realtime']
) }}

WITH base AS (

    SELECT
        contract_address,
        latest_block
    FROM
        {{ ref('silver__relevant_contracts') }}

{% if is_incremental() %}
WHERE
    contract_address NOT IN (
        SELECT
            contract_address
        FROM
            {{ this }}
    )
{% endif %}
LIMIT
    500
), function_sigs AS (
    SELECT
        '0x313ce567' AS function_sig,
        'decimals' AS function_name
    UNION
    SELECT
        '0x06fdde03',
        'name'
    UNION
    SELECT
        '0x95d89b41',
        'symbol'
),
all_reads AS (
    SELECT
        *
    FROM
        base
        JOIN function_sigs
        ON 1 = 1
),
ready_reads AS (
    SELECT
        contract_address,
        latest_block,
        function_sig,
        RPAD(
            function_sig,
            64,
            '0'
        ) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': contract_address, 'from': null, 'data': input}, utils.udf_int_to_hex(latest_block)],
            concat_ws(
                '-',
                contract_address,
                input,
                latest_block
            )
        ) AS rpc_request
    FROM
        all_reads
),
batch_reads AS (
    SELECT
        ARRAY_AGG(rpc_request) AS batch_rpc_request
    FROM
        ready_reads
),
node_call AS (
    SELECT
        *,
        live.udf_api(
            'POST',
            CONCAT(
                '{service}',
                '/',
                '{Authentication}'
            ),{},
            batch_rpc_request,
            'Vault/prod/ethereum/quicknode/mainnet'
        ) AS response
    FROM
        batch_reads
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                ready_reads
            LIMIT
                1
        )
), flat_responses AS (
    SELECT
        VALUE :id :: STRING AS call_id,
        VALUE :result :: STRING AS read_result
    FROM
        node_call,
        LATERAL FLATTEN (
            input => response :data
        )
)
SELECT
    SPLIT_PART(
        call_id,
        '-',
        1
    ) AS contract_address,
    SPLIT_PART(
        call_id,
        '-',
        3
    ) AS block_number,
    LEFT(SPLIT_PART(call_id, '-', 2), 10) AS function_sig,
    NULL AS function_input,
    read_result,
    SYSDATE() :: TIMESTAMP AS _inserted_timestamp
FROM
    flat_responses
