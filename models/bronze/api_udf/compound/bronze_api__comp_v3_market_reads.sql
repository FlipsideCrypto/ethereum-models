{{ config(
    materialized = 'incremental',
    unique_key = 'ID',
    full_refresh = false,
    tags = ['non_realtime']
) }}

WITH contract_base AS (

    SELECT
        '0xc3d688b66703497daa19211eedff47f25384cdc3' AS contract_address
),
function_sigs_market AS (
    SELECT
        '0x8285ef40' AS function_sig,
        'totalBorrow' AS function_name
    UNION
    SELECT
        '0x18160ddd' AS function_sig,
        'totalSupply' AS function_name
    UNION
    SELECT
        '0x0902f1ac' AS function_sig,
        'getReserves' AS function_name
    UNION
    SELECT
        '0x7eb71131' AS function_sig,
        'getUtilization' AS function_name
),
block_range AS (
    SELECT
        block_number
    FROM
        {{ ref('_block_ranges') }}
    WHERE
        block_number >= 15412370

{% if is_incremental() %}
AND block_number NOT IN (
    SELECT
        block_number
    FROM
        {{ this }}
)
{% endif %}
ORDER BY
    block_number DESC
LIMIT
    2000
), all_reads_market AS (
    SELECT
        *
    FROM
        contract_base
        JOIN function_sigs_market
        ON 1 = 1
        JOIN block_range
        ON 1 = 1
),
ready_reads_market AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        CONCAT(
            '[\'',
            contract_address,
            '\',',
            block_number,
            ',\'',
            function_sig,
            '\',\'\']'
        ) AS read_input
    FROM
        all_reads_market
),
batch_reads_market AS (
    SELECT
        CONCAT('[', LISTAGG(read_input, ','), ']') AS batch_read
    FROM
        ready_reads_market
),
results_market AS (
    SELECT
        ethereum.streamline.udf_json_rpc_read_calls(
            node_url,
            headers,
            PARSE_JSON(batch_read)
        ) AS read_output
    FROM
        batch_reads_market
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
                ready_reads_market
            LIMIT
                1
        )
), final_market AS (
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
        results_market,
        LATERAL FLATTEN(
            input => read_output [0] :data
        )
),
utilization_results AS (
    SELECT
        contract_address,
        block_number,
        read_result
    FROM
        final_market
    WHERE
        function_sig = '0x7eb71131'
        AND read_result IS NOT NULL
        AND read_result <> '0x0000000000000000000000000000000000000000000000000000000000000000'
),
function_sigs_rates AS (
    SELECT
        '0x9fa83b5a' AS function_sig,
        'getBorrowRate' AS function_name
    UNION
    SELECT
        '0xd955759d' AS function_sig,
        'getSupplyRate' AS function_name
),
all_reads_rates AS (
    SELECT
        *
    FROM
        utilization_results
        JOIN function_sigs_rates
        ON 1 = 1
),
ready_reads_rates AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        read_result AS function_input,
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
        all_reads_rates
),
batch_reads_rates AS (
    SELECT
        CONCAT('[', LISTAGG(read_input, ','), ']') AS batch_read
    FROM
        ready_reads_rates
),
results_rates AS (
    SELECT
        ethereum.streamline.udf_json_rpc_read_calls(
            node_url,
            headers,
            PARSE_JSON(batch_read)
        ) AS read_output
    FROM
        batch_reads_rates
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
                ready_reads_rates
            LIMIT
                1
        )
), final_rates AS (
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
        results_rates,
        LATERAL FLATTEN(
            input => read_output [0] :data
        )
),
FINAL AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_input,
        read_result
    FROM
        final_market
    UNION ALL
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_input,
        read_result
    FROM
        final_rates
)
SELECT
    contract_address,
    block_number,
    function_sig,
    function_input,
    read_result,
    SYSDATE() :: TIMESTAMP AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'function_sig']
    ) }} AS id
FROM
    FINAL
