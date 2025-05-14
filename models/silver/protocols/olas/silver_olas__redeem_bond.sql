{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_olas','curated','olas']
) }}

WITH redeem AS (

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
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        'RedeemBond' AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_1
            )
        ) AS product_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS owner_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS bond_id,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0xff8697d8d2998d6aa2e09b405795c6f4beeb0c81' --Depository
        AND topic_0 = '0x97f4572ff501a5533a09cf8d7d39be87e303470dd8bc7cff4fe91212be76bc73' --RedeemBond
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

--bonding = deposit LP tokens to get Bond that can be redeemed for OLAS tokens after maturity
),
transfers AS (
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
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        'Transfer' AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount_unadj,
        amount_unadj / pow(
            10,
            18
        ) :: FLOAT AS amount_adj,
        --amount of OLAS tokens transferred for the redemption txn, only 1 transfer per redemption txn (not 1 per RedeemBond event)
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = LOWER('0x0001A500A6B18995B03f44bb040A5fFc28E45CB0') --Autonolas: OLAS Token
        AND topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                redeem
        )
        AND tx_succeeded

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
    r.block_number,
    r.block_timestamp,
    r.tx_hash,
    r.origin_function_signature,
    r.origin_from_address,
    r.origin_to_address,
    r.contract_address,
    r.event_index,
    r.event_name,
    r.product_id,
    r.owner_address,
    r.bond_id,
    t.amount_unadj AS total_amount_redeemed_unadj,
    t.amount_adj AS total_amount_redeemed,
    r._log_id,
    r._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['r.tx_hash','r.event_index']
    ) }} AS redeem_bond_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    redeem r
    LEFT JOIN transfers t USING(tx_hash) qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
