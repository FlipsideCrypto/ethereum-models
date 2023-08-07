-- depends_on: {{ ref('bronze__streamline_confirm_blocks') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    tags = ['streamline_core_complete']
) }}

SELECT
    id,
    block_number,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_confirm_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) _inserted_timestamp
        FROM
            {{ this }})
        {% else %}
            {{ ref('bronze__streamline_FR_confirm_blocks') }}
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY id
        ORDER BY
            _inserted_timestamp DESC)) = 1
