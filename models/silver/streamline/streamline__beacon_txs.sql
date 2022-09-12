{{ config(
    materialized = 'view',
    tags = ['streamline_view']
) }}

WITH block_data AS (
    SELECT
        data
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "beacon_blocks") }}'
            )
        ) A
)

SELECT
    FLATTEN( input => data:transactions )
FROM
    block_data

{% if is_incremental() %}
AND (
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
