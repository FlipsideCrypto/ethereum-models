{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH liquidation AS(

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
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
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        CASE
            WHEN contract_address = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN contract_address = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN contract_address = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
            WHEN contract_address = LOWER('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2') THEN 'Aave V3'
            ELSE 'ERROR'
        END AS aave_version,
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
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286',
            '0x56864757fd5b1fc9f38f5f3a981cd8ae512ce41b902cf73fc506ee369c6bc237'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
AND contract_address IN(
    --Aave V2 LendingPool contract address
    LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'),
    --V2
    LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119'),
    --V1
    LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
    --AMM
    LOWER('0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2') --v3
)
AND tx_succeeded --excludes failed txs
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
        {{ ref('silver__aave_tokens') }}
),
atoken_prices AS (
    SELECT
        HOUR as prices_hour,
        token_address as underlying_address,
        symbol,
        NAME,
        decimals,
        price as hourly_price,
        blockchain,
        is_native,
        is_imputed,
        is_deprecated,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                liquidation
        )
    AND
        token_address IN (
            SELECT
                DISTINCT collateral_asset
            FROM
                liquidation
            union all
            SELECT
                DISTINCT debt_asset
            FROM
                liquidation
        )
              
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    LOWER(
        collateral_asset
    ) AS collateral_asset,
    LOWER(
        amc.atoken_address
    ) AS collateral_aave_token,
    liquidated_amount AS liquidated_amount_unadj,
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
    ) AS debt_aave_token,
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
    aave_version,
    collat.hourly_price AS collateral_token_price,
    amc.underlying_symbol AS collateral_token_symbol,
    debt.hourly_price AS debt_token_price,
    amd.underlying_symbol AS debt_token_symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS aave_liquidations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    liquidation
    LEFT JOIN atoken_meta amc
    ON liquidation.collateral_asset = amc.underlying_address
    AND liquidation.aave_version = amc.atoken_version
    LEFT JOIN atoken_prices collat
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = collat.prices_hour
    AND liquidation.collateral_asset = collat.underlying_address
    LEFT JOIN atoken_meta amd
    ON liquidation.debt_asset = amd.underlying_address
    AND liquidation.aave_version = amd.atoken_version
    LEFT JOIN atoken_prices debt
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = debt.prices_hour
    AND liquidation.debt_asset = debt.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
