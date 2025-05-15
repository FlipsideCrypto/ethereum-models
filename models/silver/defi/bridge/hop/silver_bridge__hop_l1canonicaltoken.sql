{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated']
) }}

WITH base_contracts AS (

    SELECT
        contract_address,
        MAX(block_number) AS block_number
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x0a0607688c86ec1775abcdbab7b33a3a35a6c9cde677c9be880150c231cc6b0b'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND contract_address NOT IN (
    SELECT
        DISTINCT contract_address
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1
),
function_sigs AS (
    SELECT
        '0xb7a0bda6' AS function_sig,
        'l1CanonicalToken' AS function_name
),
inputs AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_name,
        0 AS function_input,
        CONCAT(
            function_sig,
            LPAD(
                function_input,
                64,
                0
            )
        ) AS DATA
    FROM
        base_contracts
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
        function_sig,
        function_name,
        function_input,
        DATA,
        contract_address,
        block_number,
        _inserted_timestamp
    FROM
        contract_reads
)
SELECT
    read_output,
    read_id,
    read_result,
    read_id_object,
    function_sig,
    function_name,
    function_input,
    DATA,
    block_number,
    contract_address,
    CASE
        WHEN read_result IS NULL
        AND contract_address IN (
            '0xb8901acb165ed027e32754e0ffe830802919727f',
            '0x236fe0ffa7118505f2a1c35a039f6a219308b1a7'
        ) --eth_bridge
        THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE CONCAT('0x', SUBSTR(read_result, 27, 40))
    END AS token_address,
    _inserted_timestamp
FROM
    reads_flat
WHERE
    token_address <> '0x'
    AND token_address IS NOT NULL
