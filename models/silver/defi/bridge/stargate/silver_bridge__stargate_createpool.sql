{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['curated']
) }}

WITH pool_creation AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address AS contract_address,
        _call_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        from_address = '0x06d538690af257da524f25d0cd52fd85b1c2173e'
        AND TYPE ILIKE 'create%'
        AND tx_status ILIKE 'success'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND to_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
),
function_sigs AS (
    SELECT
        '0xfc0c546a' AS function_sig,
        'token' AS function_name
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
        pool_creation
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
    contract_address AS pool_address,
    block_number,
    function_sig,
    function_name,
    CONCAT('0x', SUBSTR(read_result, 27, 40)) AS token_address,
    _inserted_timestamp
FROM
    reads_flat
    LEFT JOIN function_sigs USING(function_sig)
