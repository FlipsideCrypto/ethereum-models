{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

CREATE FUNCTION evm_blocks (layer2_input TEXT)
  RETURNS TABLE (block_number INTEGER, _inserted_timestamp INTEGER)
  AS
  $$
  SELECT
    block_number,
    layer2
  FROM
    ref('evm_blocks')
  WHERE
    layer2 = layer2_input
  ORDER BY _INSERTED_TIMESTAMP ASC, BLOCK_NUMBER ASC
  $$
;