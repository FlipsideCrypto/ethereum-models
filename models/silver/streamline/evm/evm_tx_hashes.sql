{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

create function evm_tx_hashes (layer2_input text)
  returns table (block_number integer, _inserted_timestamp integer)
  as
  $$
    SELECT
      FLATTEN(A:data:transactions) as tx_hash,
      row_number() OVER(ORDER BY _INSERTED_TIMESTAMP ASC, tx_hash ASC) as row_number,
    FROM
      TABLE(
        information_schema.external_table_files(
          table_name => '{{ source( "bronze_streamline", "streamline__evm_blocks_' || layer2_input || '") }}'
        )
      ) A
    WHERE
      A:layer2 = layer2_input
    ORDER BY 
      _INSERTED_TIMESTAMP ASC, tx_hash ASC
  $$
;
