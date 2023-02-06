{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH base_data AS (

    SELECT
        block_number,
        VALUE
    FROM
        {{ source(
            'bronze_streamline',
            'blocks'
        ) }}

{% if is_incremental() %}
WHERE
    (
        block_number >= COALESCE(
            (
                SELECT
                    MAX(block_number)
                FROM
                    {{ this }}
            ),
            '1900-01-01'
        )
    )
{% endif %}
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number']
    ) }} AS id,
    block_number,
    REPLACE(CONCAT_WS('', '0x', TO_CHAR(block_number, 'XXXXXXXX')), ' ', '') AS block_number_hex
FROM
    base_data
