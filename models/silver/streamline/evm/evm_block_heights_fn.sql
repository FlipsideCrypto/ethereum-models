{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

CREATE FUNCTION evm_block_heights_fn (layer2_input TEXT)
  RETURNS TABLE (layer2 TEXT, _inserted_timestamp INTEGER)
  AS
  $$
  SELECT,
    layer2_input as layer2,
    SYSDATE() as request_time
  $$
;