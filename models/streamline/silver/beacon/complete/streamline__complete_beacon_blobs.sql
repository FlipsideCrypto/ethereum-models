-- depends on: {{ ref('bronze__streamline_beacon_blobs') }}
{{ config (
    materialized = "incremental",
    unique_key = "slot_number",
    cluster_by = "ROUND(slot_number, -3)",
    merge_update_columns = ["slot_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(slot_number)",
    incremental_predicates = ["dynamic_range", "slot_number"],
    tags = ['streamline_beacon_complete']
) }}

SELECT
    COALESCE(
        VALUE :"SLOT_NUMBER" :: INT,
        metadata :request :"slot_number" :: INT,
        PARSE_JSON(
            metadata :request :"slot_number"
        ) :: INT
    ) AS slot_number,
    --parse slot_number from metadata for FR because it's not properly accessible in VALUE column from v1 requests
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number']
    ) }} AS complete_beacon_blobs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_beacon_blobs') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(MAX(_inserted_timestamp), '1970-01-01' :: TIMESTAMP) _inserted_timestamp
        FROM
            {{ this }})
            AND (LEFT(
                DATA :error :: STRING,
                1
            ) <> 'F'
            OR DATA :error IS NULL
        )
        {% else %}
            {{ ref('bronze__streamline_fr_beacon_blobs') }}
        WHERE
            (LEFT(
                DATA :error :: STRING,
                1
            ) <> 'F'
            OR DATA :error IS NULL
        )
        {% endif %}

        qualify(ROW_NUMBER() over (PARTITION BY slot_number
        ORDER BY
            _inserted_timestamp DESC)) = 1

{% if not is_incremental() %}
UNION
SELECT
    slot_number,
    {{ dbt_utils.generate_surrogate_key(
        ['slot_number']
    ) }} AS complete_beacon_blobs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__blob_sidecars') }}
WHERE
    LEFT(
        resp :error :: STRING,
        1
    ) <> 'F'
    OR resp :error IS NULL
{% endif %}