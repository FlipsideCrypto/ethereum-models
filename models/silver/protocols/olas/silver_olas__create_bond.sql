{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

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
        'CreateBond' AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS token_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_2
            )
        ) AS product_id,
        CONCAT('0x', SUBSTR(topic_3, 27, 40)) AS owner_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS bond_id,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount_olas_unadj,
        --OLAS token minted
        amount_olas_unadj / pow(
            10,
            18
        ) :: FLOAT AS amount_olas_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) AS token_amount_unadj,
        --LP token (pair/pool)
        token_amount_unadj / pow(
            10,
            18
        ) :: FLOAT AS token_amount_adj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS maturity,
        TO_TIMESTAMP(maturity) AS maturity_timestamp,
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
        AND topic_0 = '0xfa892a5592accfded99f9df9ad2d678230f37aeeb8fde5dd3b73769b5069bc0e' --CreateBond
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
    owner_address,
    '0x0001a500a6b18995b03f44bb040a5ffc28e45cb0' AS token_address,
    amount_olas_unadj AS olas_amount_unadj,
    amount_olas_adj AS olas_amount,
    amount_olas_adj * p.price AS olas_amount_usd,
    C.token_address AS lp_token_address,
    token_amount_unadj AS lp_token_amount_unadj,
    token_amount_adj AS lp_token_amount,
    product_id,
    bond_id,
    maturity_timestamp,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS create_bond_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_evt C
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON p.token_address = '0x0001a500a6b18995b03f44bb040a5ffc28e45cb0'
    AND DATE_TRUNC(
        'hour',
        C.block_timestamp
    ) = p.hour
