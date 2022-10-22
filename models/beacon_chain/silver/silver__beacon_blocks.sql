{{ config(
    materialized = 'incremental',
    unique_key = 'slot_number',
    cluster_by = ['slot_timestamp::date'],
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
                table_name => '{{ source( "bronze_streamline", "beacon_blocks") }}'
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
{% endif %}
SELECT
    slot_number,
    DATA :message :body :attestations [0] :data :target :epoch :: INTEGER AS epoch_number,
    -- status???
    TO_TIMESTAMP(
        DATA :message :body :execution_payload :timestamp :: INTEGER
    ) AS slot_timestamp,
    DATA :message :proposer_index :: INTEGER AS proposer_index,
    -- will need to link to other source for validator via the index above
    -- missing blockRoot Hash?
    DATA :message :parent_root :: STRING AS parent_root,
    DATA :message :state_root :: STRING AS state_root,
    DATA :message :body :randao_reveal :: STRING AS randao_reveal,
    DATA :message :body :graffiti :: STRING AS graffiti,
    DATA :message :body :eth1_data :block_hash :: STRING AS eth1_block_hash,
    DATA :message :body :eth1_data :deposit_count :: INTEGER AS eth1_deposit_count,
    DATA :message :body :eth1_data :deposit_root :: STRING AS eth1_deposit_root,
    DATA :message :body :execution_payload AS execution_payload,
    DATA :signature :: STRING AS signature,
    DATA :message :body :attester_slashings AS attester_slashings,
    DATA :message :body :proposer_slashings AS proposer_slashings,
    DATA :message :body :deposits AS deposits,
    DATA :message :body :attestations AS attestations,
    OBJECT_DELETE(
        DATA :message :body,
        'attestations',
        DATA :message :body,
        'deposits'
    ) AS slot_json,
    m._inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
    DATA
FROM
    {{ source(
        "bronze_streamline",
        "beacon_blocks"
    ) }}
    s
    JOIN meta m
    ON m.file_name = metadata $ filename qualify(ROW_NUMBER() over (PARTITION BY slot_number
ORDER BY
    _inserted_timestamp DESC)) = 1
