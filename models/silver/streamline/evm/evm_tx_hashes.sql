{{ config(
    materialized = 'incremental',
    tags = ['streamline_view']
) }}

SELECT
  FLATTEN(A:data:transactions) as tx_hash,
  A:layer2 as layer2
FROM
  TABLE(
    information_schema.external_table_files(
      table_name => '{{ source( "bronze_streamline", "streamline__evm_blocks") }}'
    )
  ) A
