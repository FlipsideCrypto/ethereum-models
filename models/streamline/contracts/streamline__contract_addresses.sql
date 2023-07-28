{{ config(
    materialized = 'view'
) }}

SELECT
    DISTINCT to_address AS contract_address,
    block_number,
    SYSDATE() AS _inserted_timestamp
FROM
    {{ ref('silver__traces') }}
WHERE
    TYPE LIKE '%CREATE%'
    AND contract_address IS NOT NULL

{% if is_incremental() %}
AND (
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
        ),
        '1900-01-01'
    )
)
{% endif %}
