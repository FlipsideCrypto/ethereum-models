{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

SELECT
    last_modified,
    data:message:state_root as state_root,
    data:block_number as block_number,
    file_name
FROM
    TABLE(
        information_schema.external_table_files(
            table_name => '{{ source( "bronze_streamline", "beacon_blocks") }}'
        )
    ) A