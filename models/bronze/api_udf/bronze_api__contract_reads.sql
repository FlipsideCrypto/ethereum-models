{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    full_refresh = false
) }}

WITH base AS (

    SELECT
        contract_address,
        first_block AS created_block
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
    250
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
        created_block,
        function_sig,
        CONCAT(
            '[\'',
            contract_address,
            '\',',
            created_block,
            ',\'',
            function_sig,
            '\',\'\']'
        ) AS read_input
    FROM
        all_reads
),
batch_reads AS (
    SELECT
        CONCAT('[', LISTAGG(read_input, ','), ']') AS batch_read
    FROM
        ready_reads
),
results AS (
    SELECT
        ethereum.streamline.udf_json_rpc_read_calls(
            node_url,
            headers,
            PARSE_JSON(batch_read)
        ) AS read_output
    FROM
        batch_reads
        JOIN {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
        ON 1 = 1
        AND chain = 'ethereum'
    WHERE
        EXISTS (
            SELECT
                1
            FROM
                ready_reads
            LIMIT
                1
        )
), FINAL AS (
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
        read_id_object [3] :: STRING AS function_input
    FROM
        results,
        LATERAL FLATTEN(
            input => read_output [0] :data
        )
)
SELECT
    contract_address,
    block_number,
    function_sig,
    function_input,
    read_result,
    SYSDATE() :: TIMESTAMP AS _inserted_timestamp
FROM
    FINAL
