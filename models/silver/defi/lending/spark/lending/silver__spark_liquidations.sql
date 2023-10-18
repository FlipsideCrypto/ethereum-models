{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curation']
) }}
WITH liquidation AS(

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS collateralAsset_1,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS debtAsset_1,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS debt_to_cover_amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS liquidated_amount,
        CASE
            WHEN topics [0] :: STRING = '0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286' THEN CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40))
            ELSE CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40))
        END AS liquidator_address,
        _log_id,
        _inserted_timestamp,
        CASE
            WHEN contract_address = LOWER('0xC13e21B648A5Ee794902342038FF3aDAB66BE987') THEN 'Spark'
            ELSE 'ERROR'
        END AS spark_version,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        CASE
            WHEN debtAsset_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE debtAsset_1
        END AS debt_asset,
        CASE
            WHEN collateralAsset_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE collateralAsset_1
        END AS collateral_asset
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286',
            '0x56864757fd5b1fc9f38f5f3a981cd8ae512ce41b902cf73fc506ee369c6bc237'
            
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
AND contract_address = LOWER('0xC13e21B648A5Ee794902342038FF3aDAB66BE987')
AND tx_status = 'SUCCESS' --excludes failed txs
),
atoken_meta AS (
    SELECT
        atoken_address,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__spark_tokens') }}
),
atoken_prices AS (
    SELECT
        prices_hour,
        underlying_address,
        atoken_address,
        atoken_version,
        eth_price,
        oracle_price,
        backup_price,
        underlying_decimals,
        underlying_symbol,
        value_ethereum,
        hourly_price
    FROM
        {{ ref('silver__spark_token_prices') }}
    WHERE
        prices_hour :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                liquidation
        )
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    LOWER(
        collateral_asset
    ) AS collateral_asset,
    LOWER(
        amc.atoken_address
    ) AS collateral_spark_token,
    liquidated_amount / pow(
        10,
        amc.atoken_decimals
    ) AS liquidated_amount,
    liquidated_amount * collat.hourly_price / pow(
        10,
        amc.atoken_decimals
    ) AS liquidated_amount_usd,
    LOWER(
        debt_asset
    ) AS debt_asset,
    LOWER(
        amd.atoken_address
    ) AS debt_spark_token,
    debt_to_cover_amount / pow(
        10,
        amd.underlying_decimals
    ) AS debt_to_cover_amount,
    debt_to_cover_amount * debt.hourly_price / pow(
        10,
        amd.underlying_decimals
    ) AS debt_to_cover_amount_usd,
    liquidator_address AS liquidator,
    borrower_address AS borrower,
    spark_version as platform,
    collat.hourly_price AS collateral_token_price,
    amc.underlying_symbol AS collateral_token_symbol,
    debt.hourly_price AS debt_token_price,
    amd.underlying_symbol AS debt_token_symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    liquidation
    LEFT JOIN atoken_meta amc
    ON liquidation.collateral_asset = amc.underlying_address
    AND liquidation.spark_version = amc.atoken_version
    LEFT JOIN atoken_prices collat
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = collat.prices_hour
    AND liquidation.collateral_asset = collat.underlying_address
    LEFT JOIN atoken_meta amd
    ON liquidation.debt_asset = amd.underlying_address
    AND liquidation.spark_version = amd.atoken_version
    LEFT JOIN atoken_prices debt
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = debt.prices_hour
    AND liquidation.debt_asset = debt.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
