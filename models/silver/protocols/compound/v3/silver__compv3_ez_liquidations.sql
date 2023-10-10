{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH liquidations AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS compound_market,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS asset,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS absorber,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS collateral_absorbed,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS usd_value,
        origin_from_address AS depositor_address,
        'Compound V3' AS compound_version,
        C.name,
        C.symbol,
        C.decimals,
        'ethereum' AS blockchain,
        _log_id,
        l._inserted_timestamp
    FROM
        ethereum_dev.silver.logs l
        LEFT JOIN ethereum_dev.silver.contracts C
        ON asset = address
    WHERE
        topics [0] = '0x9850ab1af75177e4a9201c65a2cf7976d5d28e40ef63494b44366f86b2f9412e' --AbsorbCollateral

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    compound_market,
    asset,
    absorber,
    borrower,
    collateral_absorbed / pow(
        10,
        decimals
    ) AS tokens_seized,
    usd_value / pow(
        10,
        8
    ) AS liquidation_amount,
    depositor_address,
    compound_version,
    NAME as collateral_token_name,
    symbol as collateral_token_symbol,
    decimals as collateral_token_decimals,
    a.compound_market_name as debt_token_name,
    a.compound_market_symbol as debt_token_symbol,
    a.underlying_asset_address as debt_asset,
    blockchain,
    l._log_id,
    l._inserted_timestamp
FROM
    liquidations l
LEFT JOIN 
    {{ref('silver__compv3_asset_details')}} a
ON
    l.compound_market = a.compound_market_address