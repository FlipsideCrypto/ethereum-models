{{ config(
    materialized = 'incremental',
    unique_key = 'contract_address',
    tags = ['non_realtime']
) }}

WITH

{% if is_incremental() %}
heal_table AS (

    SELECT
        contract_address
    FROM
        {{ this }}
    WHERE
        token_name IS NULL
        OR len(REGEXP_REPLACE(token_name, '[^a-zA-Z0-9]+')) <= 0
        OR token_symbol IS NULL
        OR len(REGEXP_REPLACE(token_symbol, '[^a-zA-Z0-9]+')) <= 0
        OR token_decimals IS NULL
),
{% endif %}

reads_base_metadata AS (
    SELECT
        contract_address,
        block_number,
        function_sig AS function_signature,
        function_input,
        read_result AS read_output,
        _inserted_timestamp
    FROM
        {{ ref('bronze_api__contract_reads') }}
    WHERE
        read_result IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
OR contract_address IN (
    SELECT
        DISTINCT contract_address
    FROM
        heal_table
)
{% endif %}
),
uni_base_metadata AS (
    SELECT
        *
    FROM
        {{ ref('silver__reads') }}
    WHERE
        function_signature IN (
            '0x06fdde03',
            '0x313ce567',
            '0x95d89b41'
        )
        AND call_name = 'uni_v3_token_reads'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
OR contract_address IN (
    SELECT
        DISTINCT contract_address
    FROM
        heal_table
)
{% endif %}
),
base_metadata AS (
    SELECT
        contract_address,
        block_number,
        function_signature,
        function_input,
        read_output,
        _inserted_timestamp
    FROM
        reads_base_metadata
    UNION ALL
    SELECT
        contract_address,
        block_number,
        function_signature,
        function_input,
        read_output,
        _inserted_timestamp
    FROM
        uni_base_metadata
),
token_names AS (
    SELECT
        contract_address,
        block_number,
        function_signature,
        read_output,
        utils.udf_hex_to_string(SUBSTR(read_output,(64*2+3),LEN(read_output))) AS token_name
    FROM
        base_metadata
    WHERE
        function_signature = '0x06fdde03'
    AND
        token_name <> ''
),
token_symbols AS (
    SELECT
        contract_address,
        block_number,
        function_signature,
        read_output,
        utils.udf_hex_to_string(SUBSTR(read_output,(64*2+3),LEN(read_output))) AS token_symbol
    FROM
        base_metadata
    WHERE
        function_signature = '0x95d89b41'
    AND 
        token_symbol <> ''
),
token_decimals AS (
    SELECT
        contract_address,
        udf_hex_to_int(read_output) AS token_decimals
    FROM
        base_metadata
    WHERE
        function_signature = '0x313ce567' qualify(ROW_NUMBER() over(PARTITION BY contract_address
    ORDER BY
        token_decimals DESC)) = 1
),
contracts AS (
    SELECT
        contract_address,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        base_metadata
    GROUP BY
        1
)
SELECT
    c1.contract_address AS contract_address,
    token_name,
    token_decimals,
    token_symbol,
    CASE
        WHEN token_name IS NULL
        OR len(REGEXP_REPLACE(token_name, '[^a-zA-Z0-9]+')) <= 0
        OR token_decimals IS NULL
        OR token_symbol IS NULL
        OR len(REGEXP_REPLACE(token_symbol, '[^a-zA-Z0-9]+')) <= 0 THEN 'incomplete'
        ELSE 'complete'
    END AS complete_f,
    _inserted_timestamp
FROM
    contracts c1
    LEFT JOIN token_names
    ON c1.contract_address = token_names.contract_address
    LEFT JOIN token_symbols
    ON c1.contract_address = token_symbols.contract_address
    LEFT JOIN token_decimals
    ON c1.contract_address = token_decimals.contract_address qualify(ROW_NUMBER() over(PARTITION BY c1.contract_address
ORDER BY
    _inserted_timestamp DESC)) = 1
