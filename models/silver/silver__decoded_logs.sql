{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["_log_id"],
    full_refresh = false
) }}

{% if is_incremental() %}
WITH meta AS (

    SELECT
        job_created_time,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                table_name => '{{ source( "bronze_streamline", "decoded_logs") }}',
                start_time => (
                    SELECT
                        MAX(_INSERTED_TIMESTAMP)
                    FROM
                        {{ this }}
                )
            )
        )
),
date_partitions AS (
    SELECT
        DISTINCT TO_DATE(
            concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
        ) AS _partition_by_created_date
    FROM
        meta
),
block_partitions AS (
    SELECT
        DISTINCT CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 6), '_', 1) AS INTEGER) AS _partition_by_block_number
    FROM
        meta
)
{% else %}
    WITH meta AS (
        SELECT
            registered_on AS job_created_time,
            last_modified,
            file_name
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", "decoded_logs") }}'
                )
            ) A
    )
{% endif %},
decoded_logs AS (
    SELECT
        block_number :: INTEGER AS block_number,
        SPLIT(
            id,
            '-'
        ) [0] :: STRING AS tx_hash,
        SPLIT(
            id,
            '-'
        ) [1] :: INTEGER AS event_index,
        DATA :name :: STRING AS event_name,
        LOWER(
            DATA :address :: STRING
        ) :: STRING AS contract_address,
        DATA AS decoded_data,
        id :: STRING AS _log_id,
        TO_TIMESTAMP_NTZ(SYSDATE()) AS _inserted_timestamp
    FROM
        {{ source(
            "bronze_streamline",
            "decoded_logs"
        ) }} AS s
        JOIN meta b
        ON b.file_name = metadata$filename

{% if is_incremental() %}
JOIN date_partitions p
ON p._partition_by_created_date = s._partition_by_created_date
JOIN block_partitions bp
ON bp._partition_by_block_number = s._partition_by_block_number
WHERE
    s._partition_by_created_date IN (
        CURRENT_DATE,
        CURRENT_DATE -1
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
),
transformed_logs AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        contract_address,
        event_name,
        decoded_data,
        _inserted_timestamp,
        _log_id,
        silver.udf_transform_logs(decoded_data) AS transformed
    FROM
        decoded_logs
),
FINAL AS (
    SELECT
        b.tx_hash,
        b.block_number,
        b.event_index,
        b.event_name,
        b.contract_address,
        b.decoded_data,
        transformed,
        b._log_id,
        b._inserted_timestamp,
        OBJECT_AGG(
            DISTINCT CASE
                WHEN v.value :name = '' THEN CONCAT(
                    'anonymous_',
                    v.index
                )
                ELSE v.value :name
            END,
            v.value :value
        ) AS decoded_flat
    FROM
        transformed_logs b,
        LATERAL FLATTEN(
            input => transformed :data
        ) v
    GROUP BY
        b.tx_hash,
        b.block_number,
        b.event_index,
        b.event_name,
        b.contract_address,
        b.decoded_data,
        transformed,
        b._log_id,
        b._inserted_timestamp
)
SELECT
    tx_hash,
    block_number,
    event_index,
    event_name,
    contract_address,
    decoded_data,
    transformed,
    _log_id,
    _inserted_timestamp,
    decoded_flat
FROM
    FINAL
