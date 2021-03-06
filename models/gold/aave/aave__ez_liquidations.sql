{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_hash, event_index)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'ethereum', 'aave', 'aave_liquidations', 'address_labels']
) }}

WITH atokens AS(

    SELECT
        inputs :_reserve :: STRING AS reserve_token,
        A.value :: STRING AS balances,
        CASE
            WHEN contract_address IN(
                LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),
                LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d')
            ) THEN 'Aave V2'
            WHEN contract_address IN(
                LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
                LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba')
            ) THEN 'Aave AMM'
            ELSE 'Aave V1'
        END AS aave_version
    FROM
        {{source('flipside_silver_ethereum','reads')}},
        LATERAL FLATTEN(input => SPLIT(value_string, '^')) A
    WHERE
        1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= CURRENT_DATE - 2
{% else %}
    AND block_timestamp :: DATE >= CURRENT_DATE - 720
{% endif %}
AND contract_address IN (
    LOWER('0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d'),
    -- AAVE V2 Data Provider (per docs)
    LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb'),
    -- AAVE AMM Lending Pool (per docs)
    LOWER('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9'),
    -- AAVE V2 Lending Pool (per docs)
    LOWER('0xc443AD9DDE3cecfB9dfC5736578f447aFE3590ba'),
    -- AAVE AMM Data Provider (per docs)
    LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119')
) -- AAVE V1
),
underlying AS(
    SELECT
        CASE
            WHEN reserve_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE reserve_token
        END AS token_contract,
        aave_version,
        MAX(
            CASE
                WHEN SPLIT(
                    balances,
                    ':'
                ) [0] :: STRING = 'aTokenAddress' THEN SPLIT(
                    balances,
                    ':'
                ) [1]
                ELSE ''
            END
        ) AS aave_token
    FROM
        atokens
    WHERE
        1 = 1
    GROUP BY
        1,
        2
),
-- implementing aave oracle prices denominated in wei
ORACLE AS(
    SELECT
        --block_timestamp,
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS block_hour,
        inputs :address :: STRING AS token_address,
        AVG(value_numeric) AS value_ethereum -- values are given in wei and need to be converted to ethereum
    FROM
       {{source('flipside_silver_ethereum','reads')}}
    WHERE
        1 = 1
        AND contract_address = '0xa50ba011c48153de246e5192c8f9258a2ba79ca9' -- check if there is only one oracle
        AND block_timestamp :: DATE >= CURRENT_DATE - 720
    GROUP BY
        1,
        2
),
-- wen we don't have oracle pricces we use ethereum__token_prices_hourly as a backup
backup_prices AS(
    SELECT
        token_address,
        HOUR,
        decimals,
        symbol,
        AVG(price) AS price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        1 = 1

{% if is_incremental() %}
AND HOUR :: DATE >= CURRENT_DATE - 2
{% else %}
    AND HOUR :: DATE >= CURRENT_DATE - 720
{% endif %}
GROUP BY
    1,
    2,
    3,
    4
),
-- decimals backup
decimals_backup AS(
    SELECT
        address AS token_address,
        decimals,
        symbol,
        NAME
    FROM
         {{ ref('core__dim_contracts') }}
    WHERE
        1 = 1
        AND decimals IS NOT NULL
),
prices_hourly AS(
    SELECT
        underlying.aave_token,
        LOWER(
            underlying.token_contract
        ) AS token_contract,
        underlying.aave_version,
        (ORACLE.value_ethereum / pow(10,(18 - dc.decimals))) * eth_prices.price AS oracle_price,
        backup_prices.price AS backup_price,
        ORACLE.block_hour AS oracle_hour,
        backup_prices.hour AS backup_prices_hour,
        eth_prices.price AS eth_price,
        dc.decimals AS decimals,
        dc.symbol,
        value_ethereum
    FROM
        underlying
        LEFT JOIN ORACLE
        ON LOWER(
            underlying.token_contract
        ) = LOWER(
            ORACLE.token_address
        )
        LEFT JOIN backup_prices
        ON LOWER(
            underlying.token_contract
        ) = LOWER(
            backup_prices.token_address
        )
        AND ORACLE.block_hour = backup_prices.hour
        LEFT JOIN {{ ref('core__fact_hourly_token_prices') }}
        eth_prices
        ON ORACLE.block_hour = eth_prices.hour
        AND eth_prices.token_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        LEFT JOIN decimals_backup dc
        ON LOWER(
            underlying.token_contract
        ) = LOWER(
            dc.token_address
        )
),
coalesced_prices AS(
    SELECT
        prices_hourly.decimals AS decimals,
        prices_hourly.symbol AS symbol,
        prices_hourly.aave_token AS aave_token,
        prices_hourly.token_contract AS token_contract,
        prices_hourly.aave_version AS aave_version,
        COALESCE(
            prices_hourly.oracle_price,
            prices_hourly.backup_price
        ) AS coalesced_price,
        COALESCE(
            prices_hourly.oracle_hour,
            prices_hourly.backup_prices_hour
        ) AS coalesced_hour
    FROM
        prices_hourly
),
-- daily avg price used when hourly price is missing (it happens a lot)
prices_daily_backup AS(
    SELECT
        token_address,
        symbol,
        DATE_TRUNC(
            'day',
            HOUR
        ) AS block_date,
        AVG(price) AS avg_daily_price,
        MAX(decimals) AS decimals
    FROM
        backup_prices
    WHERE
        1 = 1
    GROUP BY
        1,
        2,
        3
),
--liquidations to Aave LendingPool contract
liquidation AS(
    --need to fix aave v1
    SELECT
        DISTINCT block_number,
        block_timestamp,
        event_index,
        CASE
            WHEN COALESCE(
                event_inputs :collateralAsset :: STRING,
                event_inputs :_collateral :: STRING
            ) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE COALESCE(
                event_inputs :collateralAsset :: STRING,
                event_inputs :_collateral :: STRING
            )
        END AS collateral_asset,
        CASE
            WHEN COALESCE(
                event_inputs :debtAsset :: STRING,
                event_inputs :_reserve :: STRING
            ) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE COALESCE(
                event_inputs :debtAsset :: STRING,
                event_inputs :_reserve :: STRING
            )
        END AS debt_asset,
        COALESCE(
            event_inputs :debtToCover,
            event_inputs :_purchaseAmount
        ) AS debt_to_cover_amount,
        --not adjusted for decimals
        COALESCE(
            event_inputs :liquidatedCollateralAmount,
            event_inputs :_liquidatedCollateralAmount
        ) AS liquidated_amount,
        COALESCE(
            event_inputs :liquidator :: STRING,
            event_inputs :_liquidator :: STRING
        ) AS liquidator_address,
        COALESCE(
            event_inputs :user :: STRING,
            event_inputs :_user :: STRING
        ) AS borrower_address,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        tx_hash,
        CASE
            WHEN contract_address = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN contract_address = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN contract_address = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
            ELSE 'ERROR'
        END AS aave_version
    FROM
        {{ref('core__fact_event_logs')}}
    WHERE
        1 = 1

{% if is_incremental() %}
AND block_timestamp :: DATE >= CURRENT_DATE - 2
{% else %}
    AND block_timestamp :: DATE >= CURRENT_DATE - 720
{% endif %}
AND contract_address IN(
    --Aave V2 LendingPool contract address
    LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'),
    --V2
    LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119'),
    --V1
    LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb')
) --AMM
AND event_name = 'LiquidationCall' --this is a liquidation
AND tx_status = 'SUCCESS' --excludes failed txs
)
SELECT
    liquidation.tx_hash,
    liquidation.block_number,
    liquidation.block_timestamp,
    liquidation.event_index,
    LOWER(
        liquidation.collateral_asset
    ) AS collateral_asset,
    LOWER(
        underlying.aave_token
    ) AS collateral_aave_token,
    liquidation.liquidated_amount / pow(
        10,
        COALESCE(
            coalesced_prices.decimals,
            backup_prices.decimals,
            prices_daily_backup.decimals,
            decimals_backup.decimals,
            18
        )
    ) AS liquidated_amount,
    liquidation.liquidated_amount * COALESCE(
        coalesced_prices.coalesced_price,
        backup_prices.price,
        prices_daily_backup.avg_daily_price
    ) / pow(
        10,
        COALESCE(
            coalesced_prices.decimals,
            backup_prices.decimals,
            prices_daily_backup.decimals,
            decimals_backup.decimals,
            18
        )
    ) AS liquidated_amount_usd,
    LOWER(
        liquidation.debt_asset
    ) AS debt_asset,
    LOWER(
        underlying_debt.aave_token
    ) AS debt_aave_token,
    liquidation.debt_to_cover_amount / pow(
        10,
        COALESCE(
            coalesced_prices_debt.decimals,
            backup_prices_debt.decimals,
            prices_daily_backup_debt.decimals,
            decimals_backup_debt.decimals,
            18
        )
    ) AS debt_to_cover_amount,
    liquidation.debt_to_cover_amount * COALESCE(
        coalesced_prices_debt.coalesced_price,
        backup_prices_debt.price,
        prices_daily_backup_debt.avg_daily_price
    ) / pow(
        10,
        COALESCE(
            coalesced_prices_debt.decimals,
            backup_prices_debt.decimals,
            prices_daily_backup_debt.decimals,
            decimals_backup_debt.decimals,
            18
        )
    ) AS debt_to_cover_amount_usd,
    liquidation.liquidator_address AS liquidator,
    liquidation.borrower_address AS borrower,
    liquidation.aave_version,
    COALESCE(
        coalesced_prices.coalesced_price,
        backup_prices.price,
        prices_daily_backup.avg_daily_price
    ) AS collateral_token_price,
    COALESCE(
        coalesced_prices.symbol,
        backup_prices.symbol,
        prices_daily_backup.symbol
     ) AS collateral_token_symbol,
    COALESCE(
        coalesced_prices_debt.coalesced_price,
        backup_prices_debt.price,
        prices_daily_backup_debt.avg_daily_price
    ) AS debt_token_price,
    COALESCE(
        coalesced_prices_debt.symbol,
        backup_prices_debt.symbol,
        prices_daily_backup_debt.symbol,
        decimals_backup_debt.symbol
    ) AS debt_token_symbol,
    'ethereum' AS blockchain
FROM
    liquidation
    LEFT JOIN coalesced_prices
    ON LOWER(
        liquidation.collateral_asset
    ) = LOWER(
        coalesced_prices.token_contract
    )
    AND liquidation.aave_version = coalesced_prices.aave_version
    AND DATE_TRUNC(
        'hour',
        liquidation.block_timestamp
    ) = coalesced_prices.coalesced_hour
    LEFT JOIN backup_prices
    ON LOWER(
        liquidation.collateral_asset
    ) = LOWER(
        backup_prices.token_address
    )
    AND DATE_TRUNC(
        'hour',
        liquidation.block_timestamp
    ) = backup_prices.hour
    LEFT JOIN prices_daily_backup
    ON LOWER(
        liquidation.collateral_asset
    ) = LOWER(
        prices_daily_backup.token_address
    )
    AND DATE_TRUNC(
        'day',
        liquidation.block_timestamp
    ) = prices_daily_backup.block_date
    LEFT JOIN underlying
    ON LOWER(
        liquidation.collateral_asset
    ) = LOWER(
        underlying.token_contract
    )
    AND liquidation.aave_version = underlying.aave_version
    LEFT JOIN decimals_backup
    ON LOWER(
        liquidation.collateral_asset
    ) = LOWER(
        decimals_backup.token_address
    ) -- need to join twice to cover both collateral asset and debt asset
    LEFT JOIN coalesced_prices AS coalesced_prices_debt
    ON LOWER(
        liquidation.debt_asset
    ) = LOWER(
        coalesced_prices_debt.token_contract
    )
    AND liquidation.aave_version = coalesced_prices_debt.aave_version
    AND DATE_TRUNC(
        'hour',
        liquidation.block_timestamp
    ) = coalesced_prices_debt.coalesced_hour
    LEFT JOIN backup_prices AS backup_prices_debt
    ON LOWER(
        liquidation.debt_asset
    ) = LOWER(
        backup_prices_debt.token_address
    )
    AND DATE_TRUNC(
        'hour',
        liquidation.block_timestamp
    ) = backup_prices_debt.hour
    LEFT JOIN prices_daily_backup AS prices_daily_backup_debt
    ON LOWER(
        liquidation.debt_asset
    ) = LOWER(
        prices_daily_backup_debt.token_address
    )
    AND DATE_TRUNC(
        'day',
        liquidation.block_timestamp
    ) = prices_daily_backup_debt.block_date
    LEFT JOIN underlying AS underlying_debt
    ON LOWER(
        liquidation.debt_asset
    ) = LOWER(
        underlying_debt.token_contract
    )
    AND liquidation.aave_version = underlying_debt.aave_version
    LEFT JOIN decimals_backup AS decimals_backup_debt
    ON LOWER(
        liquidation.debt_asset
    ) = LOWER(
        decimals_backup_debt.token_address
    )
    LEFT OUTER JOIN  {{ ref('core__dim_labels') }}
    l
    ON LOWER(
        underlying.aave_token
    ) = l.address
    AND l.creator = 'flipside'
