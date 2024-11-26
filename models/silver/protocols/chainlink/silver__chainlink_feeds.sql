{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['stale']
) }}

WITH base AS (

    SELECT
        contract_address,
        block_number,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                read_output :: STRING
            )
        ) AS read_result,
        _inserted_timestamp
    FROM
        {{ ref('silver__reads') }}
    WHERE
        function_signature = '0x50d25bcd'
        AND call_name = 'chainlink_price_feed'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    contract_address,
    block_number,
    read_result,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address']
    ) }} AS id,
    id AS chainlink_feeds_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base qualify(ROW_NUMBER() over(PARTITION BY contract_address, block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
