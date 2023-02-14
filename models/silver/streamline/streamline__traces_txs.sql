{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH base_data AS (

    SELECT
        block_number,
        data:hash::string AS tx_id,
        VALUE
    FROM
        {{ source(
            'bronze_streamline',
            'transactions'
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
        ['block_number', 'tx_id']
    ) }} AS id,
    block_number,
    tx_id
FROM
    base_data
