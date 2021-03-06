{{ config(
    materialized = 'incremental',
    unique_key =  "CONCAT_WS('-', tx_hash, event_index)",
    incremental_strategy = 'delete+insert',
    tags = ['snowflake', 'ethereum', 'aave', 'aave_repayments', 'address_labels']
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
        AND block_timestamp :: DATE >= '2021-06-01'
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
        AND block_timestamp :: DATE >= '2021-01-01'
    GROUP BY
        1,
        2
),
-- wen we don't have oracle pricces we use ethereum__token_prices_hourly as a backup
backup_prices AS(
    SELECT
        token_address,
        symbol,
        HOUR,
        decimals,
        AVG(price) AS price
    FROM
       {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        1 = 1
        AND HOUR :: DATE >= '2021-01-01'
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
        LEFT JOIN  {{ ref('core__fact_hourly_token_prices') }}
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
--repayments to Aave LendingPool contract
repay AS(
    SELECT
        DISTINCT block_number,
        block_timestamp,
        event_index,
        CASE
            WHEN COALESCE(
                event_inputs :vault :: STRING,
                event_inputs :_reserve :: STRING
            ) = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE COALESCE(
                event_inputs :vault :: STRING,
                event_inputs :_reserve :: STRING
            )
        END AS aave_market,
        COALESCE(
            event_inputs :amount,
            event_inputs :_amountMinusFees
        ) AS repayed_amount,
        --not adjusted for decimals
        origin_from_address AS repayer_address,
        COALESCE(
            event_inputs :owner :: STRING,
            event_inputs :_user :: STRING
        ) AS borrower_address,
        origin_to_address AS lending_pool_contract,
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
        AND block_timestamp :: DATE >= '2021-01-01'
        AND contract_address IN(
            --Aave V2 LendingPool contract address
            LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9'),
            --V2
            LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119'),
            --V1
            LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb')
        ) --AMM
        AND event_name = 'Repay' --this is a repayment
        AND tx_status = 'SUCCESS' --excludes failed txs
)
SELECT
    repay.tx_hash,
    repay.block_number,
    repay.block_timestamp,
    repay.event_index,
    LOWER(
        repay.aave_market
    ) AS aave_market,
    LOWER(
        underlying.aave_token
    ) AS aave_token,
    repay.repayed_amount / pow(
        10,
        COALESCE(
            coalesced_prices.decimals,
            backup_prices.decimals,
            prices_daily_backup.decimals,
            decimals_backup.decimals,
            18
        )
    ) AS repayed_tokens,
    repay.repayed_amount * COALESCE(
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
    ) AS repayed_usd,
    repay.repayer_address AS payer,
    repay.borrower_address AS borrower,
    LOWER(
        repay.lending_pool_contract
    ) AS lending_pool_contract,
    repay.aave_version,
    COALESCE(
        coalesced_prices.coalesced_price,
        backup_prices.price,
        prices_daily_backup.avg_daily_price
    ) AS token_price,
    COALESCE(
        coalesced_prices.symbol,
        backup_prices.symbol,
        prices_daily_backup.symbol,
        REGEXP_REPLACE(
            l.address,
            'AAVE.*: a',
            ''
        )
    ) AS symbol,
    'ethereum' AS blockchain
FROM
    repay
    LEFT JOIN coalesced_prices
    ON LOWER(
        repay.aave_market
    ) = LOWER(
        coalesced_prices.token_contract
    )
    AND repay.aave_version = coalesced_prices.aave_version
    AND DATE_TRUNC(
        'hour',
        repay.block_timestamp
    ) = coalesced_prices.coalesced_hour
    LEFT JOIN backup_prices
    ON LOWER(
        repay.aave_market
    ) = LOWER(
        backup_prices.token_address
    )
    AND DATE_TRUNC(
        'hour',
        repay.block_timestamp
    ) = backup_prices.hour
    LEFT JOIN prices_daily_backup
    ON LOWER(
        repay.aave_market
    ) = LOWER(
        prices_daily_backup.token_address
    )
    AND DATE_TRUNC(
        'day',
        repay.block_timestamp
    ) = prices_daily_backup.block_date
    LEFT JOIN underlying
    ON LOWER(
        repay.aave_market
    ) = LOWER(
        underlying.token_contract
    )
    AND repay.aave_version = underlying.aave_version
    LEFT JOIN decimals_backup
    ON LOWER(
        repay.aave_market
    ) = LOWER(
        decimals_backup.token_address
    )
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON LOWER(
        underlying.aave_token
    ) = l.address
    AND l.creator = 'flipside' qualify(ROW_NUMBER() over(PARTITION BY tx_hash, event_index
ORDER BY
    block_number ASC)) = 1
