{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "block_timestamp::date"
) }}

WITH meta AS (

    SELECT
        job_created_time,
        last_modified,
        TO_DATE(
            concat_ws('-', SPLIT_PART(file_name, '/', 3), SPLIT_PART(file_name, '/', 4), SPLIT_PART(file_name, '/', 5))
        ) AS _partition_by_created_date,
        CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 6), '_', 1) AS INTEGER) AS _partition_by_block_number,
        file_name
    FROM
        TABLE(
            information_schema.external_table_file_registration_history(
                table_name => '{{ source( "streamline_test", "decoded_logs_test") }}',
                start_time => DATEADD('day', -7, CURRENT_TIMESTAMP())
            )
        )
),
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
            "streamline_test",
            "decoded_logs_test"
        ) }} AS s
        JOIN meta b
        ON b.file_name = metadata$filename
        AND b._partition_by_created_date = s._partition_by_created_date
        AND b._partition_by_block_number = s._partition_by_block_number
    WHERE
        b._partition_by_created_date = s._partition_by_created_date
        AND b._partition_by_block_number = s._partition_by_block_number qualify(ROW_NUMBER() over (PARTITION BY _log_id
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
        ethereum.silver.udf_transform_logs(decoded_data) AS transformed
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
),
new_records AS (
    SELECT
        b.tx_hash,
        b.block_number,
        b.event_index,
        b.event_name,
        b.contract_address,
        b.decoded_data,
        b.transformed,
        b._log_id,
        b._inserted_timestamp,
        b.decoded_flat,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        DATA,
        event_removed,
        tx_status,
        CASE
            WHEN block_timestamp IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending
    FROM
        FINAL b
        LEFT JOIN {{ ref('silver__logs') }} USING (
            block_number,
            _log_id
        )
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.tx_hash,
        t.block_number,
        t.event_index,
        t.event_name,
        t.contract_address,
        t.decoded_data,
        t.transformed,
        t._log_id,
        GREATEST(
            t._inserted_timestamp,
            l._inserted_timestamp
        ) AS _inserted_timestamp,
        t.decoded_flat,
        l.block_timestamp,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.topics,
        l.data,
        l.event_removed,
        l.tx_status,
        FALSE AS is_pending
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__logs') }}
        l USING (
            block_number,
            _log_id
        )
    WHERE
        t.is_pending
        AND l.block_timestamp IS NOT NULL
)
{% endif %}
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
    decoded_flat,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    is_pending
FROM
    new_records

{% if is_incremental() %}
UNION
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
    decoded_flat,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    is_pending
FROM
    missing_data
{% endif %}
