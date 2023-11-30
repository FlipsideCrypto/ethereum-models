{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_events AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae'
        AND contract_address IN (
            '0x253553366da8546fc250f225fe3d25d0c782303b',
            '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5',
            '0x82994379b1ec951c8e001dfcec2a7ce8f4f39b97',
            '0xa271897710a2b22f7a5be5feacb00811d960e0b8',
            '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85',
            '0xfac7bea255a6990f749363002136af6556b31e04',
            '0xf0ad5cad05e10572efceb849f6ff0c68f9700455',
            '0xb22c1c159d12461ea124b0deb4b5b93020e6ad16'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    origin_from_address AS manager,
    decoded_flat :"name" :: STRING AS NAME,
    decoded_flat :"label" :: STRING AS label,
    TRY_TO_NUMBER(
        decoded_flat :"cost" :: STRING
    ) AS cost_raw,
    cost_raw / pow(
        10,
        18
    ) AS cost,
    decoded_flat :"expires" :: STRING AS expires,
    TRY_TO_TIMESTAMP(expires) AS expires_timestamp,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS ens_domain_renewals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_events
