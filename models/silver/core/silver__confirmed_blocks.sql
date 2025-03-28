-- depends_on: {{ ref('bronze__confirm_blocks') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = "round(block_number,-3)",
    tags = ['realtime']
) }}

WITH base AS (

    SELECT
        block_number,
        DATA :result :hash :: STRING AS block_hash,
        DATA :result :transactions AS txs,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__confirm_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            IFNULL(
                MAX(
                    _inserted_timestamp
                ),
                '1970-01-01' :: TIMESTAMP
            ) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__confirm_blocks_fr') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    block_number,
    block_hash,
    VALUE :: STRING AS tx_hash,
    _inserted_timestamp
FROM
    base,
    LATERAL FLATTEN (
        input => txs
    )
