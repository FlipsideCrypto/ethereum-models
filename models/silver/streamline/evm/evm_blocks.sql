{{ config(
    materialized = 'incremental',
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
      l2s.NAME as layer2,
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
          l2s.HOST
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