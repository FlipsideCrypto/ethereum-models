{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH all_collected AS (

    SELECT
        *
    FROM
        {{ ref('silver__logs') }}
    WHERE
        block_timestamp :: DATE > '2021-04-01'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'
        AND topics [0] :: STRING IN (
            '0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0',
            '0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
collected_base AS (
    SELECT
        *,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS vault_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS owner,
        PUBLIC.udf_hex_to_int(
            's2c',
            topics [2] :: STRING
        ) AS tick_lower,
        PUBLIC.udf_hex_to_int(
            's2c',
            topics [3] :: STRING
        ) AS tick_upper,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) AS amount0,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) AS amount1
    FROM
        all_collected
    WHERE
        topics [0] :: STRING = '0x70935338e69775456a85ddef226c395fb668b63fa0115f5f20610b388e6ca9c0'
),
nf_token_id_base AS (
    SELECT
        tx_hash,
        contract_address AS nf_position_manager_address,
        PUBLIC.udf_hex_to_int(
            's2c',
            topics [1] :: STRING
        ) AS nf_token_id,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) AS liquidity_provider,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) AS amount0,
        PUBLIC.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) AS amount1,
        event_index,
        event_index - 1 AS event_index_join
    FROM
        all_collected
    WHERE
        topics [0] :: STRING = '0x40d0efd1a53d60ecbf40971b9daf7dc90178c3aadc7aab1765632738fa8b8f01'
),
pool_data AS (
    SELECT
        token0_address,
        token1_address,
        fee,
        fee_percent,
        tick_spacing,
        pool_address
    FROM
        {{ ref('silver__univ3_pools') }}
)

SELECT
    'ethereum' AS blockchain,
    b.block_number AS block_number,
    b.block_timestamp AS block_timestamp,
    b.tx_hash AS tx_hash,
    b.event_index AS event_index,
    b.contract_address AS pool_address,
    b.origin_from_address AS liquidity_provider,
    nf_token_id,
    CASE
        WHEN nf_token_id IS NULL THEN vault_address
        ELSE nf_position_manager_address
    END AS nf_position_manager_address,
    token0_address,
    token1_address,
    b.amount0,
    b.amount1,
    b.tick_lower,
    b.tick_upper,
    _inserted_timestamp,
    _log_id
FROM
    collected_base b
    LEFT JOIN nf_token_id_base
    ON b.tx_hash = nf_token_id_base.tx_hash
    AND b.event_index = nf_token_id_base.event_index_join
    LEFT JOIN pool_data
    ON b.contract_address = pool_data.pool_address
