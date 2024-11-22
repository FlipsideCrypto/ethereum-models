{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH withdrawals AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'PendingUnstake' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS ownerAddress,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS receiverAddress,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS isAETH,
        CASE
            WHEN isAETH = 1 THEN TRUE
            ELSE FALSE
        END AS is_aeth,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xc5130045b6f6c9e2944ccea448ad17c279db68237b8aa856ee12cbfaa25f7715' --PendingUnstake
        AND contract_address = '0x84db6ee82b7cf3b47e8f19270abde5718b936670' --ankr ETH2 Staking
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
burns AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Transfer' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS VALUE,
        (VALUE / pow(10, 18)) :: FLOAT AS value_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer (Burn)
        AND contract_address = '0xe95a203b1a91a908f9b9ce46459d101078c2c3cb' --Ankr Staked ETH (ankrETH)
        AND to_address = '0x0000000000000000000000000000000000000000'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    w.block_number,
    w.block_timestamp,
    w.origin_function_signature,
    w.origin_from_address,
    w.origin_to_address,
    w.tx_hash,
    w.event_index,
    w.event_name,
    w.contract_address,
    w.ownerAddress AS sender,
    w.receiverAddress AS recipient,
    amount AS eth_amount,
    amount_adj AS eth_amount_adj,
    value AS token_amount,
    value_adj AS token_amount_adj,
    '0xe95a203b1a91a908f9b9ce46459d101078c2c3cb' AS token_address,
    'ankrETH' AS token_symbol,
    'ankr' AS platform,
    is_aeth,
    w._log_id,
    w._inserted_timestamp
FROM
    withdrawals w
    LEFT JOIN burns b
    ON w.tx_hash = b.tx_hash qualify (ROW_NUMBER() over (PARTITION BY w._log_id
ORDER BY
    w._inserted_timestamp DESC) = 1)
