{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
) }}

WITH log_join AS (

    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 42)) AS caller,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42)) AS owner,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER / pow(
            10,
            decimals
        ) AS deposit_amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER / pow(
            10,
            decimals
        ) AS deposit_shares,
        deposit_amount / NULLIF(
            deposit_shares,
            0
        ) AS deposit_share_price,
        f.*
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__fraxlend_ez_asset_details') }}
        f
        LEFT JOIN {{ ref('silver__logs') }}
        l
        ON f.frax_market_address = l.contract_address
    WHERE
        topics [0] = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7'

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
)
SELECT
    tx_hash,
    block_timestamp,
    block_number,
    caller,
    owner,
    deposit_amount,
    deposit_shares,
    deposit_share_price,
    frax_market_address,
    frax_market_symbol,
    underlying_asset,
    _log_id,
    _inserted_timestamp
FROM
    log_join
