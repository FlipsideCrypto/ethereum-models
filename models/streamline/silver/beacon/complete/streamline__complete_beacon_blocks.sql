-- depends on: {{ ref('bronze__beacon_blocks') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(slot_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    incremental_predicates = ["dynamic_range", "slot_number"],
    tags = ['streamline_beacon_complete']
) }}

SELECT
    id,
    slot_number,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__beacon_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__fr_beacon_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
