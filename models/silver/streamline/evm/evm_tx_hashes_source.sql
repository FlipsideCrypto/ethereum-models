{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

create function evm_tx_hashes (layer2_input text)
  returns table (block_number integer, _inserted_timestamp integer)
  as
  $$
    SELECT
      tx_hash,
      row_number,
      layer2
    FROM
      ref('evm_tx_hashes')
    WHERE
      layer2 = layer2_input
    ORDER BY 
      _INSERTED_TIMESTAMP ASC, tx_hash ASC
  $$
;
