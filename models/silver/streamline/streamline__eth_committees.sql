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
            'bronze_streamline',
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
),
tbl AS (
    SELECT
        'committees' AS func_type,
        slot_number,
        VALUE :data :message :state_root :: STRING AS state_id
    FROM
        base_data
    WHERE
        state_id IS NOT NULL
    UNION ALL
    SELECT
        'sync_committees' AS func_type,
        slot_number,
        VALUE :data :message :state_root :: STRING AS state_id
    FROM
        base_data
    WHERE
        state_id IS NOT NULL
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['slot_number', 'func_type']
    ) }} AS id,
    func_type,
    slot_number,
    state_id
FROM
    tbl
