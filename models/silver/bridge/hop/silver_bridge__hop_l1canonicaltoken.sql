{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    tags = ['non_realtime']
) }}

WITH base_contracts AS (

    SELECT
        contract_address,
        MAX(block_number) AS block_number
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x0a0607688c86ec1775abcdbab7b33a3a35a6c9cde677c9be880150c231cc6b0b'

{% if is_incremental() %}
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
inputs_contracts AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        (ROW_NUMBER() over (PARTITION BY contract_address
    ORDER BY
        block_number)) - 1 AS function_input
    FROM
        base_contracts
        JOIN function_sigs
        ON 1 = 1
),
contract_reads AS (
    SELECT
        ethereum.streamline.udf_json_rpc_read_calls(
            node_url,
            headers,
            PARSE_JSON(batch_read)
        ) AS read_output,
        SYSDATE() AS _inserted_timestamp
    FROM
        (
            SELECT
                CONCAT('[', LISTAGG(read_input, ','), ']') AS batch_read
            FROM
                (
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
                        ) AS read_input
                    FROM
                        inputs_contracts
                ) ready_reads_contracts
        ) batch_reads_contracts
        JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
        ON 1 = 1
        AND chain = 'ethereum'
),
reads_flat AS (
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
        contract_reads,
        LATERAL FLATTEN(
            input => read_output [0] :data
        )
)
SELECT
    contract_address,
    block_number,
    function_sig,
    function_name,
    CASE
        WHEN read_result IS NULL
        AND contract_address IN (
            '0xb8901acb165ed027e32754e0ffe830802919727f',
            '0x236fe0ffa7118505f2a1c35a039f6a219308b1a7'
        ) --eth_bridge
        THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE CONCAT('0x', SUBSTR(read_result, 27, 40))
    END AS token_address,
    SYSDATE() AS _inserted_timestamp
FROM
    reads_flat
    LEFT JOIN function_sigs USING(function_sig)
WHERE
    token_address <> '0x'
    AND token_address IS NOT NULL
