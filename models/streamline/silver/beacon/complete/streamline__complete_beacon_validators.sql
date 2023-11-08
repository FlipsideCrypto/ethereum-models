-- depends on: {{ ref('bronze__beacon_validators') }}
{{ config (
    materialized = "incremental",
    unique_key = "_id",
    cluster_by = "ROUND(slot_number, -3)",
    merge_update_columns = ["_id"],
    incremental_predicates = ["dynamic_range", "slot_number"],
    tags = ['streamline_beacon_complete']
) }}

SELECT
    MD5(
        CAST(COALESCE(CAST(block_number AS text), '') AS text)
    ) AS _id,
    block_number AS slot_number,
    state_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__beacon_validators') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    DATA NOT ILIKE '%internal server error%'
{% else %}
    {{ ref('bronze__fr_beacon_validators') }}
WHERE
    DATA NOT ILIKE '%internal server error%'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY _id
ORDER BY
    _inserted_timestamp DESC)) = 1
