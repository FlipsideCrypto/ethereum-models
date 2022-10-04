{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}


with blocks as (
        select
            row_number() over (
                order by
                    seq4()
            ) as block_number
        from
            table(generator(rowcount => 1000000000))
    )
select
    block_number,
    SYSDATE() AS _inserted_timestamp
from
    blocks
WHERE
    block_number <= UDF_GET_CHAINHEAD():: int;

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
