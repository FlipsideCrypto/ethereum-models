{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(slot_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH base_data AS (

    SELECT
        slot_number,
        VALUE
    FROM
        {{ source(
            'bronze_streamline_prod',
            'beacon_blocks'
        ) }}

{% if is_incremental() %}
WHERE
    (
        slot_number >= COALESCE(
            (
                SELECT
                    MAX(slot_number)
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
        ['slot_number']
    ) }} AS id,
    slot_number,
    VALUE :data :message :state_root :: STRING AS state_id
FROM
    base_data
WHERE
    VALUE :data :message :state_root :: STRING IS NOT NULL
