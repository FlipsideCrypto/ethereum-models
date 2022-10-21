{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

SELECT
    FLATTEN(A:data:transactions) as tx_hash,
    row_number() OVER(ORDER BY _INSERTED_TIMESTAMP ASC, tx_hash ASC) as row_number,
    A:layer2 as layer2
  FROM
    TABLE(
      information_schema.external_table_files(
        table_name => '{{ source( "bronze_streamline", "streamline__evm_blocks_' || layer2_input || '") }}'
      )
    ) A
  ORDER BY 
    _INSERTED_TIMESTAMP ASC, tx_hash ASC