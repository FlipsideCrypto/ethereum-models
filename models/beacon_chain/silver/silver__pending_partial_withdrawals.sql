-- depends on: {{ ref('bronze__streamline_beacon_pending_partial_withdrawals') }}
{{ config (
    materialized = "incremental",
    unique_key = "pending_partial_withdrawals_id",
    cluster_by = "ROUND(request_slot_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(pending_partial_withdrawals_id)",
    incremental_predicates = ["dynamic_range", "request_slot_number"],
    tags = ['silver','beacon']
) }}

SELECT
    COALESCE(
        VALUE :"SLOT_NUMBER" :: INT,
        metadata :request :"slot_number" :: INT,
        PARSE_JSON(
            metadata :request :"slot_number"
        ) :: INT
    ) AS request_slot_number,
    try_to_number(data:amount::STRING) AS amount,
    try_to_number(data:validator_index::STRING) AS validator_index,
    try_to_number(data:withdrawable_epoch::STRING) AS withdrawable_epoch,
    DATEADD(
        'seconds',
        try_to_number(data:withdrawable_epoch::STRING) * 32 * 12,
        '2020-12-01T12:00:23Z' :: timestamp_ntz
    ) AS withdrawable_epoch_timestamp,
    data,
    {{ dbt_utils.generate_surrogate_key(
        ['request_slot_number', 'validator_index', 'withdrawable_epoch', 'amount']
    ) }} AS pending_partial_withdrawals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_beacon_pending_partial_withdrawals') }}
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
            {{ ref('bronze__streamline_fr_beacon_pending_partial_withdrawals') }}
        WHERE
            (LEFT(
                DATA :error :: STRING,
                1
            ) <> 'F'
            OR DATA :error IS NULL
        )
        {% endif %}

qualify(ROW_NUMBER() over (PARTITION BY pending_partial_withdrawals_id
ORDER BY
    _inserted_timestamp DESC)) = 1

