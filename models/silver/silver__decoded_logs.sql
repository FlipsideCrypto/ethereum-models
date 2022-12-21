{{ config (
    materialized = "incremental",
    unique_key = "_log_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["_log_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(_log_id)"
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "decoded_logs") }}'
            )
        ) A

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_INSERTED_TIMESTAMP), '1970-01-01' :: DATE) max_INSERTED_TIMESTAMP
        FROM
            {{ this }})
    )
{% else %}
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
        registered_on :: TIMESTAMP AS _inserted_timestamp
    FROM
        {{ source(
            "bronze_streamline",
            "decoded_logs"
        ) }} AS s
        JOIN meta b
        ON b.file_name = metadata$filename
    WHERE
        block_number > 15000000 qualify(ROW_NUMBER() over (PARTITION BY _log_id
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
    *
FROM
    FINAL
