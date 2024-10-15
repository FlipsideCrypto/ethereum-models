{{ config(
    materialized = 'incremental',
    unique_key = 'vault_no',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['curated']
) }}

WITH vaults AS (

    SELECT
        vault_no,
        TRIM(
            to_char(
                vault_no,
                'XXXXXXXXXXXXXXXXXXX'
            )
        ) AS vault_no_hex
    FROM
        {{ ref('silver_maker__vault_creation') }}

{% if is_incremental() %}
WHERE
    vault_no NOT IN (
        SELECT
            vault_no
        FROM
            {{ this }}
    )
{% endif %}
ORDER BY
    vault_no
LIMIT
    3000
), reads_inputs AS (
    SELECT
        '0x5ef30b9986345249bc32d8928B7ee64DE9435E39' AS contract_address,
        '0x2726b073' AS function_sig
),
block_no AS (
    SELECT
        MAX(block_number) AS block_number
    FROM
        {{ ref('silver__blocks') }}
),
all_reads_market AS (
    SELECT
        vault_no,
        vault_no_hex,
        block_number,
        contract_address,
        function_sig
    FROM
        vaults
        JOIN reads_inputs
        ON 1 = 1
        JOIN block_no
        ON 1 = 1
),
ready_reads_market AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        CONCAT(function_sig, LPAD(vault_no_hex, 64, '0')) AS input,
        utils.udf_json_rpc_call(
            'eth_call',
            [{'to': contract_address, 'from': null, 'data': input}, utils.udf_int_to_hex(block_number)],
            concat_ws(
                '-',
                contract_address,
                input,
                block_number
            )
        ) AS rpc_request,
        vault_no,
        vault_no_hex
    FROM
        all_reads_market
),
batch_reads_market AS (
    SELECT
        ARRAY_AGG(rpc_request) AS batch_rpc_request
    FROM
        ready_reads_market
),
results_market AS (
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
        batch_reads_market
),
FINAL AS (
    SELECT
        VALUE :id :: STRING AS read_id,
        VALUE :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        read_id_object [0] :: STRING AS contract_address,
        read_id_object [2] :: STRING AS block_number,
        LEFT(
            read_id_object [1] :: STRING,
            10
        ) AS function_sig,
        RIGHT(
            read_id_object [1] :: STRING,
            20
        ) AS function_input
    FROM
        results_market,
        LATERAL FLATTEN(
            input => response :data
        )
)
SELECT
    read_id,
    read_result,
    read_id_object,
    contract_address,
    block_number,
    function_sig,
    function_input,
    utils.udf_hex_to_int(function_input) AS vault_no,
    CONCAT('0x', SUBSTR(read_result :: STRING, 27, 40)) AS urn_address,
    {{ dbt_utils.generate_surrogate_key(
        ['vault_no']
    ) }} AS urns_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
