{{ config(
    materialized = 'incremental',
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"]
) }}

WITH meta AS (

    SELECT
        registered_on,
        last_modified,
        LEAST(
            last_modified,
            registered_on
        ) AS _inserted_timestamp,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "bronze_streamline", "beacon_validators") }}'
            )
        )

{% if is_incremental() %}
WHERE
    LEAST(
        registered_on,
        last_modified
    ) >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: DATE) AS max_inserted_timestamp
        FROM
            {{ this }})
{% endif %}
)
SELECT
    s.block_number,
    s.state_id,
    s.INDEX,
    s.ARRAY_INDEX,
    s.DATA :balance :: INTEGER / pow(10, 9) AS balance,
    s.DATA :status :: STRING AS validator_status,
    s.DATA :validator :activation_eligibility_epoch :: INTEGER AS activation_eligibility_epoch,
    s.DATA :validator :activation_epoch :: INTEGER AS activation_epoch,
    s.DATA :validator: effective_balance :: INTEGER / pow(10, 9) AS effective_balance,
    s.DATA :validator: exit_epoch :: INTEGER AS exit_epoch,
    s.DATA :validator: pubkey :: STRING AS pubkey,
    s.DATA :validator: slashed :: BOOLEAN AS slashed,
    s.DATA :validator: withdrawable_epoch :: INTEGER AS withdrawable_epoch,
    s.DATA :validator: withdrawal_credentials :: STRING AS withdrawal_credentials,
    s.DATA :validator AS validator_details,
    _inserted_timestamp,
    {{ dbt_utils.surrogate_key(['block_number', 'index', 'array_index']) }} AS id
FROM
    {{ source(
        "bronze_streamline",
        "beacon_validators"
    ) }} s
JOIN meta m ON m.file_name = metadata$filename 
{% if is_incremental() %}
WHERE m._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }} )
{% endif %}
QUALIFY(ROW_NUMBER() over (PARTITION BY id
    ORDER BY
        _inserted_timestamp DESC)) = 1