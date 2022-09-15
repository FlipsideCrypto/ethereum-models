{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

SELECT
    block_number,
    SYSDATE() AS _inserted_timestamp
FROM
    generate_series(
        0,
        SELECT UDF_GET_CHAINHEAD()
    ) block_number
WHERE
    1 = 1

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
