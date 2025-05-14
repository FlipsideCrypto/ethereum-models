{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    cluster_by = ['slot_timestamp::date'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(pubkey,signature,withdrawal_credentials)",
    tags = ['silver','beacon']
) }}

WITH beacon_blocks AS (

    SELECT
        slot_number,
        slot_timestamp,
        epoch_number,
        deposits,
        execution_payload :block_number :: INT AS block_number,
        _inserted_timestamp
    FROM
        {{ ref('silver__beacon_blocks') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) {% if 'beacon' in var('HEAL_MODELS') %}
                - INTERVAL '240 hours'
            {% endif %}
        FROM
            {{ this }}
    )
{% endif %}
),
beacon_deposits AS (
    SELECT
        slot_number,
        slot_timestamp,
        epoch_number,
        VALUE :data :amount :: INTEGER AS deposit_amount,
        VALUE :data :pubkey :: STRING AS pubkey,
        VALUE :data :signature :: STRING AS signature,
        VALUE :data :withdrawal_credentials :: STRING AS withdrawal_credentials,
        VALUE :proof AS proofs,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['slot_number', 'signature', 'proofs']
        ) }} AS id,
        id AS beacon_deposits_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        beacon_blocks,
        LATERAL FLATTEN(
            input => deposits
        )
    WHERE
        deposits IS NOT NULL qualify(ROW_NUMBER() over (PARTITION BY id
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
logs_deposit AS (
    SELECT
        block_number,
        utils.udf_hex_to_int(
            HEX_ENCODE(
                REVERSE(
                    TO_BINARY(SUBSTR(decoded_log :amount :: STRING, 3))
                )
            )
        ) AS deposit_amount,
        utils.udf_hex_to_int(
            HEX_ENCODE(
                REVERSE(
                    TO_BINARY(SUBSTR(decoded_log :index :: STRING, 3))
                )
            )
        ) AS deposit_index,
        decoded_log :pubkey :: STRING AS pubkey,
        decoded_log :signature :: STRING AS signature,
        decoded_log :withdrawal_credentials :: STRING AS withdrawal_credentials,
        NULL AS proofs
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_number > 22430892
        AND block_timestamp :: DATE >= '2025-05-07'
        AND contract_address = '0x00000000219ab540356cbb839cbe05303d7705fa'
        AND event_name = 'DepositEvent'

{% if is_incremental() and 'beacon' not in var('HEAL_MODELS') %}
AND inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
logs_deposit_final AS (
    SELECT
        slot_number,
        slot_timestamp,
        epoch_number,
        deposit_amount,
        pubkey,
        signature,
        withdrawal_credentials,
        NULL AS proofs,
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['slot_number', 'signature', 'deposit_index']
        ) }} AS id,
        id AS beacon_deposits_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        beacon_blocks
        INNER JOIN logs_deposit USING (block_number)
)
SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    deposit_amount,
    pubkey,
    signature,
    withdrawal_credentials,
    proofs,
    _inserted_timestamp,
    id,
    beacon_deposits_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    beacon_deposits
UNION ALL
SELECT
    slot_number,
    slot_timestamp,
    epoch_number,
    deposit_amount,
    pubkey,
    signature,
    withdrawal_credentials,
    proofs,
    _inserted_timestamp,
    id,
    beacon_deposits_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    logs_deposit_final
