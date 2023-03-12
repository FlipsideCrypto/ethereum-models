{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(slot_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

WITH max_slot_number AS (

    SELECT
        MAX(slot_number) AS max_slot_number
    FROM
        {{ this }}),
        base_data AS (
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
        slot_number >= 
            (
                SELECT
                    MAX(max_slot_number)
                FROM
                    max_slot_number
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
GROUP BY 1,2,3
