{{ config (
    materialized = "incremental",
    unique_key = "CONCAT_WS('-', func_type, slot_number)",
    cluster_by = "ROUND(slot_number, -3)"
) }}

SELECT
    'committees' AS func_type,
    VALUE :data :message :state_root :: STRING AS state_id,
    slot_number
FROM
    {{ source(
        'bronze_streamline_prod',
        'beacon_blocks'
    ) }}
WHERE
    state_id IS NOT NULL
    AND slot_number NOT IN (
        SELECT
            slot_number
        FROM
            {{ this.schema }}.eth_committees_history
        WHERE
            func_type = 'committees'
    )
UNION ALL
SELECT
    'sync_committees' AS func_type,
    VALUE :data :message :state_root :: STRING AS state_id,
    slot_number
FROM
    {{ source(
        'bronze_streamline_prod',
        'beacon_blocks'
    ) }}
WHERE
    state_id IS NOT NULL
    AND slot_number NOT IN (
        SELECT
            slot_number
        FROM
            {{ this.schema }}.eth_committees_history
        WHERE
            func_type = 'sync_committees'
    )
