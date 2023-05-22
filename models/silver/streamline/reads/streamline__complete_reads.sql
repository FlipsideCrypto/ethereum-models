-- depends_on: {{ ref('bronze__reads') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    incremental_predicates = ["dynamic_range", "block_number"],
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    id,
    contract_address,
    function_signature,
    call_name,
    function_input,
    block_number,
    TO_TIMESTAMP_LTZ(_inserted_timestamp) as _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__reads') }}
WHERE
    TO_TIMESTAMP_LTZ(_inserted_timestamp) >= (
        SELECT
            DATEADD('hour', -3, MAX(_inserted_timestamp))
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__fr_reads') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY id
        ORDER BY
            _inserted_timestamp DESC)) = 1
