{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'registry_reads_id',
    full_refresh = false,
    tags = ['curated']
) }}

WITH unit_contracts AS (

    SELECT
        contract_address,
        unit_id AS registry_id,
        MAX(block_number) AS block_number
    FROM
        {{ ref('silver_olas__unit_registrations') }}

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
service_contracts AS (
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
all_contracts AS (
    SELECT
        *
    FROM
        unit_contracts
    UNION ALL
    SELECT
        *
    FROM
        service_contracts
),
function_sigs AS (
    SELECT
        '0xc87b56dd' AS function_sig,
        'tokenURI' AS function_name
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
                all_contracts
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
        )
    SELECT
        read_output,
        read_output :data :id :: STRING AS read_id,
        read_output :data :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        function_sig,
        function_name,
        function_input,
        DATA,
        utils.udf_hex_to_string(SUBSTR(read_result, 131)) AS token_uri_link,
        contract_address,
        block_number,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['contract_address','function_input']
        ) }} AS registry_reads_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        contract_reads
    WHERE
        token_uri_link IS NOT NULL
        AND LENGTH(token_uri_link) <> 0
