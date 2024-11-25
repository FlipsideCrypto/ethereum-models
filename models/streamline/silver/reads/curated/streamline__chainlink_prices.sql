{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    tags = ['stale']
) }}

WITH price_feeds AS (

    SELECT
        feed_address,
        created_block_number
    FROM
        {{ ref('silver__chainlink_feeds_seed') }}
),
block_range AS (
    -- edit this range to use a different block range from the ephemeral table
    SELECT
        block_number_50 AS block_input,
        _inserted_timestamp
    FROM
        {{ ref('_block_ranges') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
),
feeds_range AS (
    SELECT
        feed_address AS contract_address,
        block_input AS block_number,
        _inserted_timestamp,
        '0x50d25bcd' AS function_signature,
        0 AS function_input_plug
    FROM
        price_feeds
        JOIN block_range
    WHERE
        block_input IS NOT NULL
        AND block_input >= created_block_number
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'contract_address', 'function_signature', 'function_input_plug']
    ) }} AS id,
    NULL AS function_input,
    function_signature,
    block_number,
    contract_address,
    'chainlink_price_feed' AS call_name,
    _inserted_timestamp
FROM
    feeds_range qualify(ROW_NUMBER() over(PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
