-- depends on: {{ ref('bronze__streamline_beacon_validators') }}
{{ config(
    materialized = 'incremental',
    unique_key = "id",
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = "ROUND(block_number, -3)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(id, state_id, pubkey, slashed, withdrawal_credentials)",
    incremental_predicates = ["dynamic_range", "block_number"],
    tags = ['silver','beacon']
) }}

SELECT
    COALESCE(
        VALUE :"SLOT_NUMBER" :: INT,
        VALUE :"block_number" :: INT
    ) AS block_number,
    COALESCE(
        VALUE :"STATE_ID" :: STRING,
        metadata :request :"state_id" :: STRING
    ) AS state_id,
    array_index AS INDEX,
    array_index,
    DATA :balance :: INTEGER / pow(
        10,
        9
    ) AS balance,
    DATA :status :: STRING AS validator_status,
    DATA :validator :activation_eligibility_epoch :: INTEGER AS activation_eligibility_epoch,
    DATA :validator :activation_epoch :: INTEGER AS activation_epoch,
    DATA :validator: effective_balance :: INTEGER / pow(
        10,
        9
    ) AS effective_balance,
    DATA :validator: exit_epoch :: INTEGER AS exit_epoch,
    DATA :validator: pubkey :: STRING AS pubkey,
    DATA :validator: slashed :: BOOLEAN AS slashed,
    DATA :validator: withdrawable_epoch :: INTEGER AS withdrawable_epoch,
    DATA :validator: withdrawal_credentials :: STRING AS withdrawal_credentials,
    DATA :validator AS validator_details,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'index']) }} AS id,
    id AS beacon_validators_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
{% if is_incremental() %}
{{ ref('bronze__streamline_beacon_validators') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND DATA NOT ILIKE '%not found%'
    AND DATA NOT ILIKE '%internal server error%'
{% else %}
    {{ ref('bronze__streamline_fr_beacon_validators') }}
WHERE
    DATA NOT ILIKE '%not found%'
    AND DATA NOT ILIKE '%internal server error%'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1