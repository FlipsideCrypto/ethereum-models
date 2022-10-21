{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

SELECT
    FLATTEN(A:data:transactions) as tx_hash,
    row_number() OVER(ORDER BY _INSERTED_TIMESTAMP ASC, tx_hash ASC) as row_number,
    A:layer2 as layer2
  FROM
    {{ref('streamline__evm_blocks_destination')}}