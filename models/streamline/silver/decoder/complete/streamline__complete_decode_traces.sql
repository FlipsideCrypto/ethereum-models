-- depends_on: {{ ref('bronze__decoded_traces) }}
{{ config (
    materialized = "incremental",
    unique_key = "_call_id",
    cluster_by = "ROUND(block_number, -3)",
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_update_columns = ["_call_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_call_id)",
    tags = ['streamline_decoded_traces_complete']
) }}

SELECT
    block_number,
    id AS _call_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__decoded_traces) }}
WHERE
    TO_TIMESTAMP_NTZ(_inserted_timestamp) >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__fr_decoded_traces) }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
