{{ config(
    materialized = 'incremental',
    unique_key = 'vault_no',
    full_refresh = false
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
        CONCAT(
            '[\'',
            contract_address,
            '\',',
            block_number,
            ',\'',
            function_sig,
            '\',\'',
            vault_no_hex,
            '\']'
        ) AS read_input,
        vault_no,
        vault_no_hex
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
        results_market,
        LATERAL FLATTEN(
            input => read_output [0] :data
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
    PUBLIC.udf_hex_to_int(function_input) AS vault_no,
    CONCAT('0x', SUBSTR(read_result :: STRING, 27, 40)) AS urn_address
FROM
    FINAL
