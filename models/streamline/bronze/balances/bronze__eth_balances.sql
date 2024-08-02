{{ config (
    materialized = 'view'
) }}

WITH meta AS (

    SELECT
        last_modified AS _inserted_timestamp,
        file_name,
        TO_NUMBER(SPLIT_PART(file_name, '/', 3)) AS _partition_by_block_id
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                table_name => '{{ source( "bronze_streamline", "eth_balances") }}')
            ) A
        )
    SELECT
        s.block_number :: INTEGER AS block_number,
        address :: STRING AS address,
        b._inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['s.block_number', 'address']
        ) }} AS id,
        s._partition_by_block_id AS _partition_by_block_id,
        block_timestamp :: TIMESTAMP AS block_timestamp,
        s.data
    FROM
        {{ source(
            'bronze_streamline',
            'eth_balances'
        ) }}
        s
        JOIN meta b
        ON b.file_name = metadata$filename
        AND b._partition_by_block_id = s._partition_by_block_id
        JOIN {{ ref('_block_ranges') }} 
        --needed for block_timestamp column downstream 
        --> create second model/macro in fsc_utils for balances (will be useful for other chains)
        r
        ON r.block_number = s.block_number
    WHERE
        b._partition_by_block_id = s._partition_by_block_id
        AND (
            DATA :error :code IS NULL
            OR DATA :error :code NOT IN (
                '-32000',
                '-32001',
                '-32002',
                '-32003',
                '-32004',
                '-32005',
                '-32006',
                '-32007',
                '-32008',
                '-32009',
                '-32010'
            )
        )
