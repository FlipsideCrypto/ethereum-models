{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'getservice_reads_id',
    full_refresh = false,
    tags = ['curated']
) }}

WITH service_contracts AS (

    SELECT
        contract_address,
        service_id AS registry_id,
        MAX(block_number) AS block_number
    FROM
        {{ ref('silver_olas__service_registrations') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
    AND CONCAT(
        contract_address,
        '-',
        registry_id
    ) NOT IN (
        SELECT
            CONCAT(
                contract_address,
                '-',
                function_input
            )
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    1,
    2
),
function_sigs AS (
    SELECT
        '0xef0e239b' AS function_sig,
        'getService' AS function_name
),
inputs AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_name,
        registry_id AS function_input,
        CONCAT(
            function_sig,
            LPAD(
                SUBSTR(utils.udf_int_to_hex(function_input), 3),
                64,
                0)
            ) AS DATA
            FROM
                service_contracts
                JOIN function_sigs
                ON 1 = 1
        ),
        contract_reads AS (
            SELECT
                contract_address,
                block_number,
                function_sig,
                function_name,
                function_input,
                DATA,
                utils.udf_json_rpc_call(
                    'eth_call',
                    [{ 'to': contract_address, 'from': null, 'data': data }, utils.udf_int_to_hex(block_number) ]
                ) AS rpc_request,
                live.udf_api(
                    'POST',
                    CONCAT(
                        '{service}',
                        '/',
                        '{Authentication}'
                    ),{},
                    rpc_request,
                    'Vault/prod/ethereum/quicknode/mainnet'
                ) AS read_output,
                SYSDATE() AS _inserted_timestamp
            FROM
                inputs
        ),
        reads_flat AS (
            SELECT
                read_output,
                read_output :data :id :: STRING AS read_id,
                read_output :data :result :: STRING AS read_result,
                SPLIT(
                    read_id,
                    '-'
                ) AS read_id_object,
                regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_read,
                utils.udf_hex_to_int(
                    VALUE :: STRING
                ) AS decoded_read,
                function_sig,
                function_name,
                function_input,
                DATA,
                contract_address,
                block_number,
                _inserted_timestamp
            FROM
                contract_reads,
                LATERAL FLATTEN(
                    input => segmented_read
                )
        ),
        reads_final AS (
            SELECT
                read_output,
                read_id,
                read_result,
                read_id_object,
                segmented_read,
                function_sig,
                function_name,
                function_input,
                DATA,
                contract_address,
                block_number,
                _inserted_timestamp,
                ARRAY_AGG(TRY_TO_NUMBER(decoded_read)) AS reads_array,
                ARRAY_SLICE(reads_array, 9, ARRAY_SIZE(reads_array)) AS agent_ids
            FROM
                reads_flat
            GROUP BY
                ALL)
            SELECT
                *,
                {{ dbt_utils.generate_surrogate_key(
                    ['contract_address','function_input']
                ) }} AS getservice_reads_id,
                SYSDATE() AS inserted_timestamp,
                SYSDATE() AS modified_timestamp,
                '{{ invocation_id }}' AS _invocation_id
            FROM
                reads_final
            WHERE
                agent_ids IS NOT NULL
                AND agent_ids :: STRING <> '[]'
