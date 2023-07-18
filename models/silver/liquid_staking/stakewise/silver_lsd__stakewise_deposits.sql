{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH transfers AS (

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
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer (Stake)
        AND contract_address = '0xfe2e637202056d30016725477c5da089ab0a043a' --StakeWise Staked ETH2 (sETH2)
        AND from_address = '0x0000000000000000000000000000000000000000'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
referrers AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'StakedWithReferrer' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS referrer_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x2f6cbf015ae7f747147c62891da8bad454d58bd9fe94d218ecc3dbbfbd48c16e' --StakedWithReferrer
        AND contract_address = '0xc874b064f465bdd6411d45734b56fac750cda29a' --Stakewise: ETH2 Staking

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
SELECT
    t.block_number,
    t.block_timestamp,
    t.origin_function_signature,
    t.origin_from_address,
    t.origin_to_address,
    t.tx_hash,
    t.event_index,
    t.event_name,
    t.contract_address,
    t.from_address AS sender,
    t.to_address AS recipient,
    r.referrer_address,
    t.value AS eth_amount,
    t.value_adj AS eth_amount_adj,
    eth_amount AS token_amount,
    eth_amount_adj AS token_amount_adj,
    '0xfe2e637202056d30016725477c5da089ab0a043a' AS token_address,
    'sETH2' AS token_symbol,
    'stakewise' AS platform,
    t._log_id,
    t._inserted_timestamp
FROM
    transfers t
    LEFT JOIN referrers r
    ON t.tx_hash = r.tx_hash qualify (ROW_NUMBER() over (PARTITION BY t._log_id
ORDER BY
    t._inserted_timestamp DESC)) = 1
)

SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    event_name,
    contract_address,
    sender,
    recipient,
    referrer_address,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    token_address,
    token_symbol,
    platform,
    _log_id,
    _inserted_timestamp
FROM
    FINAL
WHERE eth_amount <> 0