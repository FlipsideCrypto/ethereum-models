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
    block_number,
    block_timestamp,
    event_index,
    segmented_data,
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
    NAME,
    symbol,
    decimals,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    liquidations
