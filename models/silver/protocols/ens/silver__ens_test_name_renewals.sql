{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH base_events AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_status,
        contract_address,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topics,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0x253553366da8546fc250f225fe3d25d0c782303b',
            --ETHRegistrarController
            '0x283af0b28c62c092c9727f1ee09c02ca627eb7f5' --Old ETHRegistrarController
        )
        AND topics [0] :: STRING IN (
            '0x3da24c024582931cfaf8267d8ed24d13a82a8068d5bd337d30ec45cea4e506ae' --NameRenewed
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_status,
    contract_address,
    DATA,
    segmented_data,
    topics,
    topics [1] :: STRING AS label,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        )
    ) AS cost,
    cost / pow(
        10,
        18
    ) AS cost_adj,
    utils.udf_hex_to_int(
        segmented_data [2] :: STRING
    ) AS expires,
    TRY_TO_TIMESTAMP(expires) AS expires_timestamp,
    utils.udf_hex_to_string(
        segmented_data [4] :: STRING
    ) AS NAME,
    _log_id,
    _inserted_timestamp
FROM
    base_events