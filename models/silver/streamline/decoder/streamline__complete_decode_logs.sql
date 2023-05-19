-- depends_on: {{ ref('bronze__decoded_logs') }}
{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_update_columns = ["_log_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_log_id)"
) }}

SELECT
    block_number,
    id AS _log_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__decoded_logs') }}
WHERE
    TO_TIMESTAMP_NTZ(_inserted_timestamp) >= (
        SELECT
            DATEADD('hour', -3, MAX(_inserted_timestamp))
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__fr_decoded_logs') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY id
        ORDER BY
            _inserted_timestamp DESC)) = 1
