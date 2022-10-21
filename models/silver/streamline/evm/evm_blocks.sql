{{ config(
    materialized = 'incremental',
    tags = ['streamline_view']
) }}

with blocks as (
  select
    row_number() over (order by seq4()) as block_number
  from
    table(generator(rowcount => 1000000000))
  )
select
    row_number() OVER(ORDER BY _INSERTED_TIMESTAMP ASC, BLOCK_NUMBER ASC) as row_number,
    l2.L2_NAME as layer_2,
    block_number,
    SYSDATE() AS _inserted_timestamp,
from
    blocks
cross join
    {{ ref('evm_layer2s') }} l2s
where
    block_number <= js_hex_to_int(
      UDF_CALL_NODE(
        {
          'jsonrpc': '2.0',
          'method': 'eth_blockNumber',
          'params': [],
          'id': 0
        },
        host_input
      ):result :: Text
    ) :: int;

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