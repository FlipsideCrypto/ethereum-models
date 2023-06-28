{{ config(
    materialized = 'table',
    unique_key = 'tx_hash',
    cluster_by = ['block_timestamp::DATE']
) }}
--only using this for tx_hash where cause filter and one earliest equery, used for a lot of the CTE's tho some incremental here should work its way down. 
--Will need to updated the earlierst query though, can probably use incremental logic there so just pull from model
WITH mintshare_burnshare AS (
    SELECT
        tx_hash,
        block_timestamp,
        '0x' || SUBSTR(
            input,
            35,
            40
        ) AS "Wallet Address"
    FROM
        {{ ref('silver__traces') }}
    WHERE
        (
            input ILIKE '0x1a378f0d%'
            OR input ILIKE '0xc2f04b0a%'
        )
        AND to_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        AND tx_status = 'SUCCESS'
        AND block_timestamp :: DATE >= '2023-04-01' --set this to April 23 for testing, original was 1/1/2022
        {% if is_incremental() %}
        AND _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp) _inserted_timestamp
                FROM
                    {{ this }}
            )
        {% endif %}
),
snx_price_feeds AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        decoded_log :current * 1e -8 AS snx_price
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address = '0x06ce8be8729b6ba18dd3416e3c223a5d4db5e755'
        AND event_name = 'AnswerUpdated'
        AND block_timestamp :: DATE >= '2022-01-01'
    ORDER BY
        block_timestamp
),
sds_price_feeds AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        decoded_log :current * 1e -27 AS sds_price
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address = '0xc7bb32a4951600fbac701589c73e219b26ca2dfc'
        AND event_name = 'AnswerUpdated'
        AND block_timestamp :: DATE >= '2022-01-01'
    ORDER BY
        block_timestamp DESC
),
snx_balance AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        '0x' || SUBSTR(
            input,
            35,
            40
        ) AS "Wallet Address",
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER * 1e -18 AS "SNX Balance",
        trace_index
    FROM
        {{ ref('silver__traces') }}
    WHERE
        to_address = '0x5b1b5fea1b99d83ad479df0c222f0492385381dd'
        AND input ILIKE '0x70a08231%'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                mintshare_burnshare
        )
        AND "Wallet Address" IN (
            SELECT
                "Wallet Address"
            FROM
                mintshare_burnshare
        )
        AND block_timestamp :: DATE >= '2022-01-01' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            "Wallet Address"
            ORDER BY
                trace_index
        ) = 1
),
snxescrow_balance AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        '0x' || SUBSTR(
            input,
            35,
            40
        ) AS "Wallet Address",
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER * 1e -18 AS "Escrowed SNX Balance"
    FROM
        {{ ref('silver__traces') }}
    WHERE
        to_address IN (
            '0xb671f2210b1f6621a2607ea63e6b2dc3e2464d1f',
            '0xac86855865cbf31c8f9fbb68c749ad5bd72802e3',
            '0xda4ef8520b1a57d7d63f1e249606d1a459698876'
        )
        AND input ILIKE '0x70a08231%'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                mintshare_burnshare
        )
        AND block_timestamp :: DATE >= '2022-01-01'
),
sds_balance AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        '0x' || SUBSTR(
            input,
            35,
            40
        ) AS "Wallet Address",
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER * 1e -18 AS "SDS Balance"
    FROM
        {{ ref('silver__traces') }}
    WHERE
        to_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        AND input ILIKE '0x70a08231%'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                mintshare_burnshare
        )
        AND block_timestamp :: DATE >= '2022-01-01' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            "Wallet Address"
            ORDER BY
                trace_index
        ) = 1
),
--getting the Collateralization Ratio
l1_target_cratios AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        (1 /(decoded_log :newRatio :: DECIMAL * 1e -18)) * 100 AS "Target C-Ratio"
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        event_name = 'IssuanceRatioUpdated'
    ORDER BY
        block_timestamp DESC
),

--converting mint/burns into +/-  
sds_mints_burns AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        decoded_log :account AS "Wallet Address",
        origin_from_address,
        (
            CASE
                WHEN event_name = 'Burn' THEN -1 * decoded_log :amount :: DECIMAL * 1e -18
                WHEN event_name = 'Mint' THEN decoded_log :amount :: DECIMAL * 1e -18
            END
        ) AS "Minted Amount",
        event_index
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                mintshare_burnshare
        )
        AND event_name IN (
            'Burn',
            'Mint'
        )
        AND contract_address IN ('0x89fcb32f29e509cc42d0c8b6f058c993013a843f')
        AND block_timestamp :: DATE >= '2022-01-01'
),
susd_mints_burns AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        (
            CASE
                WHEN decoded_log :to = '0x0000000000000000000000000000000000000000' THEN decoded_log :from
                WHEN decoded_log :from = '0x0000000000000000000000000000000000000000' THEN decoded_log :to
            END
        ) AS "Wallet Address",
        (
            CASE
                WHEN decoded_log :to = '0x0000000000000000000000000000000000000000' THEN -1 * decoded_log :value :: FLOAT * 1e -18
                WHEN decoded_log :from = '0x0000000000000000000000000000000000000000' THEN decoded_log :value :: FLOAT * 1e -18
            END
        ) AS "Minted Amount",
        event_index
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address = '0x57ab1ec28d129707052df4df418d58a2d46d5f51'
        AND (
            decoded_log :from = '0x0000000000000000000000000000000000000000'
            OR decoded_log :to = '0x0000000000000000000000000000000000000000'
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                mintshare_burnshare
        )
        AND block_timestamp :: DATE >= '2022-01-01'
),
snx_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        decoded_log :from AS from_address,
        decoded_log :to AS to_address,
        decoded_log :value :: FLOAT * 1e -18 AS trf_value
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address = '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f'
        AND (
            decoded_log :from IN (
                SELECT
                    "Wallet Address"
                FROM
                    sds_mints_burns
            )
            OR decoded_log :to IN (
                SELECT
                    "Wallet Address"
                FROM
                    sds_mints_burns
            )
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                sds_mints_burns
        )
        AND event_name = 'Transfer'
        AND block_timestamp :: DATE >= '2022-01-01'
),
bal_with_snxtrf_raw1 AS (
    SELECT
        bal.*,
        trf.from_address,
        trf.to_address,
        (
            CASE
                WHEN bal."Wallet Address" = trf.to_address THEN trf.trf_value
                WHEN bal."Wallet Address" = trf.from_address THEN -1 * trf.trf_value
                ELSE 0
            END
        ) AS snx_trf_value
    FROM
        snx_balance bal
        LEFT JOIN snx_transfers trf
        ON bal.tx_hash = trf.tx_hash
),
bal_with_snxtrf_raw2 AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        "Wallet Address",
        "SNX Balance",
        SUM(snx_trf_value) over (
            PARTITION BY tx_hash,
            "Wallet Address"
            ORDER BY
                block_timestamp
        ) AS net_trf_amount
    FROM
        bal_with_snxtrf_raw1 qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            "Wallet Address"
            ORDER BY
                block_timestamp
        ) = 1
),
bal_with_snxtrf AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        "Wallet Address",
        (
            CASE
                WHEN "SNX Balance" + net_trf_amount < 0 THEN 0
                ELSE "SNX Balance" + net_trf_amount
            END
        ) AS "SNX Balance"
    FROM
        bal_with_snxtrf_raw2
),
bal_with_sds_trf_raw AS (
    SELECT
        bal.tx_hash,
        bal.block_number,
        bal.block_timestamp,
        bal."Wallet Address",
        trf.event_name,
        bal."SDS Balance" AS initial_balance,
        trf."Minted Amount",
        SUM(
            trf."Minted Amount"
        ) over (
            PARTITION BY bal.tx_hash,
            bal."Wallet Address"
            ORDER BY
                trf.event_index
        ) AS net_sum,
        bal."SDS Balance" + net_sum AS "SDS Balance",
        trf.event_index
    FROM
        sds_balance bal
        JOIN sds_mints_burns trf
        ON bal.tx_hash = trf.tx_hash
        AND bal."Wallet Address" = trf."Wallet Address"
),
bal_with_sds_trf AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        "Wallet Address",
        event_name,
        "Minted Amount",
        "SDS Balance",
        event_index
    FROM
        bal_with_sds_trf_raw
),
add_sdsprice_raw AS (
    SELECT
        bal.*,
        sdsprice.sds_price,
        sdsprice.block_timestamp AS sdsprice_timestamp
    FROM
        bal_with_sds_trf bal
        JOIN sds_price_feeds sdsprice
        ON bal.block_timestamp >= sdsprice.block_timestamp
),
add_sdsprice1 AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        "Wallet Address",
        event_name,
        "Minted Amount",
        "SDS Balance",
        sds_price,
        event_index
    FROM
        add_sdsprice_raw qualify ROW_NUMBER() over (
            PARTITION BY "Wallet Address",
            tx_hash,
            "Minted Amount",
            event_name
            ORDER BY
                sdsprice_timestamp DESC
        ) = 1
),
absent_sdsbalances AS (
    SELECT
        *
    FROM
        bal_with_sds_trf
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                add_sdsprice1
        )
),
absent_tx_eventlogs AS (
    SELECT
        *
    FROM
        {{ref("silver__decoded_logs")}}
    WHERE
        contract_address IN (
            '0x57ab1ec28d129707052df4df418d58a2d46d5f51',
            '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        )
        AND (
            decoded_log :from IN (
                SELECT
                    "Wallet Address"
                FROM
                    absent_sdsbalances
            )
            OR decoded_log :to IN (
                SELECT
                    "Wallet Address"
                FROM
                    absent_sdsbalances
            )
        )
        AND (
            decoded_log :from = '0x0000000000000000000000000000000000000000'
            OR decoded_log :to = '0x0000000000000000000000000000000000000000'
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                absent_sdsbalances
        )
        AND block_timestamp :: DATE >= '2022-01-01'
),
absent_tx_sds AS (
    SELECT
        *
    FROM
        absent_tx_eventlogs
    WHERE
        contract_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index DESC
        ) = 1
),
absent_tx_susd AS (
    SELECT
        *
    FROM
        absent_tx_eventlogs
    WHERE
        contract_address = '0x57ab1ec28d129707052df4df418d58a2d46d5f51'
),
absent_sdsprices_data AS (
    SELECT
        sds.block_timestamp,
        sds.tx_hash,
        susd.event_index AS susd_evtIndex,
        sds.event_index AS sds_evtIndex,
        susd.decoded_log :value :: DECIMAL * 1e -18 AS susd_amount,
        sds.decoded_log :value :: DECIMAL * 1e -18 AS sds_amount,
        susd_amount / sds_amount AS sds_price
    FROM
        absent_tx_sds sds
        JOIN absent_tx_susd susd
        ON sds.tx_hash = susd.tx_hash
        AND sds.event_index < susd.event_index
    WHERE
        sds_amount != 0 qualify ROW_NUMBER() over (
            PARTITION BY sds.tx_hash
            ORDER BY
                susd.event_index
        ) = 1
),
add_sdsprice2 AS (
    SELECT
        sdsbal.tx_hash,
        sdsbal.block_number,
        sdsbal.block_timestamp,
        sdsbal."Wallet Address",
        sdsbal.event_name,
        sdsbal."Minted Amount",
        sdsbal."SDS Balance",
        sdsprice.sds_price,
        sdsbal.event_index
    FROM
        absent_sdsbalances sdsbal
        JOIN absent_sdsprices_data sdsprice
        ON sdsbal.tx_hash = sdsprice.tx_hash
),
add_sdsprice AS (
    SELECT
        *
    FROM
        add_sdsprice1
    UNION
    SELECT
        *
    FROM
        add_sdsprice2
),
combined_balances_sdsprice_raw AS (
    SELECT
        sds.block_number,
        sds.block_timestamp,
        sds.tx_hash,
        sds.event_name,
        sds."Minted Amount",
        sds."Wallet Address",
        snx."SNX Balance",
        snxes."Escrowed SNX Balance",
        sds."SDS Balance",
        sds.sds_price
    FROM
        add_sdsprice sds
        JOIN bal_with_snxtrf snx
        ON sds.tx_hash = snx.tx_hash
        AND sds."Wallet Address" = snx."Wallet Address"
        JOIN snxescrow_balance snxes
        ON sds.tx_hash = snxes.tx_hash
        AND sds."Wallet Address" = snxes."Wallet Address" qualify ROW_NUMBER() over (
            PARTITION BY sds.tx_hash,
            sds."Wallet Address"
            ORDER BY
                sds.block_timestamp,
                sds.event_index DESC
        ) = 1
),
------------------------------- imported_addresses CTEs
imported_address_traces AS (
    SELECT
        *
    FROM
        {{ ref('silver__traces') }}
    WHERE
        input ILIKE '0x8f849518%'
),
imported_address_mints AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_name,
        decoded_log :account AS "Wallet Address",
        (
            decoded_log :amount
        ) :: INTEGER * 1e -18 AS "Minted Amount"
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                imported_address_traces
        )
        AND tx_status = 'SUCCESS'
        AND event_name = 'Mint'
),
imported_address_escrow_balance_raw AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        '0x' || SUBSTR(
            input,
            35,
            40
        ) AS "Wallet Address",
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER * 1e -18 AS "Escrowed SNX Amount"
    FROM
        {{ ref('silver__traces') }}
    WHERE
        to_address IN (
            '0xb671f2210b1f6621a2607ea63e6b2dc3e2464d1f',
            '0xac86855865cbf31c8f9fbb68c749ad5bd72802e3',
            '0xda4ef8520b1a57d7d63f1e249606d1a459698876'
        )
        AND input ILIKE '0x70a08231%' -- AND input ILIKE '%64f78b5a3a8767031b8c282140aa167bab3fbe85'
    ORDER BY
        block_timestamp
),
imported_address_SNX_balance_raw AS (
    SELECT
        block_number,
        block_timestamp,
        user_address AS "Wallet Address",
        current_bal_unadj * 1e -18 AS "SNX Balance Amount"
    FROM
        {{("ez_balance_deltas")}}
    WHERE
        block_timestamp <= '2022-02-09 05:01:00.000'
        AND user_address IN (
            SELECT
                "Wallet Address"
            FROM
                imported_address_mints
        )
        AND (symbol IN ('SNX', 'HAV')
        OR contract_address = '0xc011a72400e58ecd99ee497cf89e3775d4bd732f') qualify ROW_NUMBER() over (
            PARTITION BY user_address
            ORDER BY
                block_timestamp DESC
        ) = 1
    ORDER BY
        block_timestamp
),
imported_address_escrow_balance AS (
    SELECT
        *
    FROM
        imported_address_escrow_balance_raw
    WHERE
        "Wallet Address" IN (
            SELECT
                "Wallet Address"
            FROM
                imported_address_mints
        )
        AND block_timestamp <= '2022-02-09 05:01:00.000' qualify ROW_NUMBER() over (
            PARTITION BY "Wallet Address"
            ORDER BY
                block_timestamp DESC
        ) = 1
    ORDER BY
        block_timestamp
),
absent_addresses_snx AS (
    SELECT
        *
    FROM
        imported_address_mints
    WHERE
        "Wallet Address" NOT IN (
            SELECT
                "Wallet Address"
            FROM
                imported_address_SNX_balance_raw
        )
),
imported_address_SNX_balance_raw1 AS (
    SELECT
        block_number,
        block_timestamp,
        "Wallet Address",
        0 AS "SNX Balance Amount"
    FROM
        absent_addresses_snx
),
imported_address_SNX_balance AS (
    SELECT
        *
    FROM
        imported_address_SNX_balance_raw
    UNION
    SELECT
        *
    FROM
        imported_address_SNX_balance_raw1
),
imported_combined_balances_raw AS (
    SELECT
        sds.block_number,
        sds.block_timestamp,
        sds.tx_hash,
        sds."Wallet Address",
        snx."SNX Balance Amount" AS "SNX Balance",
        esc."Escrowed SNX Amount" AS "Escrowed SNX Balance",
        sds."Minted Amount" AS "SDS Balance"
    FROM
        imported_address_mints sds
        LEFT JOIN imported_address_SNX_balance snx
        ON sds."Wallet Address" = snx."Wallet Address"
        LEFT JOIN imported_address_escrow_balance esc
        ON esc."Wallet Address" = snx."Wallet Address"
),
imported_combined_balances AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        'Mint' AS event_name,
        "SDS Balance" AS "Minted Amount",
        "Wallet Address",
        "SNX Balance",
        (
            CASE
                WHEN "Escrowed SNX Balance" IS NULL THEN 0
                ELSE "Escrowed SNX Balance"
            END
        ) AS "Escrowed SNX Balance",
        "SDS Balance"
    FROM
        imported_combined_balances_raw
),
earliest_burn_mintshare_txn AS (
    {% if is_incremental() %}
        SELECT 
            tx_hash,
            Wallet_Address,
            block_timestamp
        FROM
            {{ this }}
        ORDER BY 
            block_timestamp
        LIMIT
            1
    {% else %}
    SELECT
        *
    FROM
        mintshare_burnshare
    ORDER BY
        block_timestamp
    LIMIT
        1
    {% endif %}
), sole_sds_price AS (
    SELECT
        (
            SELECT
                raw_amount * 1e -18
            FROM
                {{ ref("silver__transfers") }}
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        earliest_burn_mintshare_txn
                )
                AND contract_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        ) AS sds_amount,
        (
            SELECT
                raw_amount * 1e -18
            FROM
                {{ ref("silver__transfers") }}
            WHERE
                tx_hash IN (
                    SELECT
                        tx_hash
                    FROM
                        earliest_burn_mintshare_txn
                )
                AND contract_address = '0x57ab1ec28d129707052df4df418d58a2d46d5f51'
        ) AS susd_amount,
        (
            SELECT
                block_timestamp
            FROM
                earliest_burn_mintshare_txn
        ) AS block_timestamp,
        susd_amount / sds_amount AS sds_price
),
imported_combined_balances_sdsprice AS (
    SELECT
        bal.*,
        sdsprice.sds_price
    FROM
        imported_combined_balances bal
        JOIN sole_sds_price sdsprice
    WHERE
        bal.block_timestamp != sdsprice.block_timestamp
),
------------------------------- Final L1 Staking Table CTEs
combined_balances_sdsprice AS (
    SELECT
        *
    FROM
        combined_balances_sdsprice_raw
    UNION
    SELECT
        *
    FROM
        imported_combined_balances_sdsprice
),
combined_balances_prices AS (
    SELECT
        bal.*,
        snxprice.snx_price
    FROM
        combined_balances_sdsprice bal
        JOIN snx_price_feeds snxprice
        ON bal.block_timestamp >= snxprice.block_timestamp qualify ROW_NUMBER() over (
            PARTITION BY bal."Wallet Address",
            bal.tx_hash
            ORDER BY
                snxprice.block_timestamp DESC
        ) = 1
),
combined_balances_prices_final AS (
    SELECT
        *,
        (
            CASE
                WHEN "SDS Balance" = 0 THEN 0
                ELSE (
                    (("SNX Balance" + "Escrowed SNX Balance") * snx_price) /(
                        "SDS Balance" * sds_price
                    )
                ) * 100
            END
        ) AS "Account C-Ratio"
    FROM
        combined_balances_prices
)
SELECT
    bal.*,
    cratio."Target C-Ratio"
FROM
    combined_balances_prices_final bal
    JOIN l1_target_cratios cratio
    ON bal.block_timestamp >= cratio.block_timestamp qualify ROW_NUMBER() over (
        PARTITION BY bal.tx_hash,
        bal."Wallet Address",
        bal."SDS Balance"
        ORDER BY
            cratio.block_timestamp DESC
    ) = 1
ORDER BY
    block_timestamp DESC
