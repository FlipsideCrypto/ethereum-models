{{ config(
    materialized = 'incremental',
    tags = ['streamline_view']
) }}


with heights as (
  select 
    MAX(js_hex_to_int(A:response:result) :: int) as height,
    A:request:layer2 as layer2
  from
    {{ref('evm_block_heights_destination')}}
  group by layer2
),
with num_range as (
  select
    row_number() over (order by seq4()) as block_number
  from
    table(generator(rowcount => 1000000000))
)
select
    h.layer2 as layer_2,
    block_number,
    SYSDATE() AS _inserted_timestamp,
from
    num_range r
cross join
    heights h
where
    block_number <= h.height;

{% if is_incremental() %}
    and (
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
ORDER BY _INSERTED_TIMESTAMP ASC, BLOCK_NUMBER ASC