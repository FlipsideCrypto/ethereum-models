{{ config(
    materialized = 'incremental',
    unique_key = 'wallet_tx_hash',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH applicable_traces AS (

        SELECT
            tx_hash,
            to_address,
            block_number,
            block_timestamp,
            '0x' || SUBSTR(
                input,
                35,
                40
            ) AS wallet_address,
            input,
            output,
            trace_index,
            _inserted_timestamp
        FROM
            {{ ref('silver__traces') }}
        WHERE
            (
                input ILIKE '0x1a378f0d%'
                OR 
                input ILIKE '0xc2f04b0a%'
                OR
                input ILIKE '0x70a08231%'
                OR
                input ILIKE '0x8f849518%'
            )
            AND to_address IN (
                '0xb671f2210b1f6621a2607ea63e6b2dc3e2464d1f',
                '0xac86855865cbf31c8f9fbb68c749ad5bd72802e3',
                '0xda4ef8520b1a57d7d63f1e249606d1a459698876',
                '0x5b1b5fea1b99d83ad479df0c222f0492385381dd',
                '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
            )
            AND tx_status = 'SUCCESS'
            AND block_timestamp::DATE >= '2022-01-01'
            {% if is_incremental() %}
            AND _inserted_timestamp >= (
                SELECT
                    MAX(traces_timestamp)
                FROM
                    {{ this }}
            )
            {% endif %}
),
applicable_logs AS (
    SELECT
        *
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address IN (
            '0x06ce8be8729b6ba18dd3416e3c223a5d4db5e755',
            '0xc7bb32a4951600fbac701589c73e219b26ca2dfc',
            '0x89fcb32f29e509cc42d0c8b6f058c993013a843f',
            '0x57ab1ec28d129707052df4df418d58a2d46d5f51',
            '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f',
            '0x57ab1ec28d129707052df4df418d58a2d46d5f51',
            '0x89fcb32f29e509cc42d0c8b6f058c993013a843f',
            '0x202ae40bed1640b09e2af7ac5719d129a498b7c8',
            '0x5ad055a1f8c936fb0deb7024f1539bb3eaa8dc3e',
            '0xdd3c1c64402a679e8d709fccf606bd77ee12b567',
            '0xa4339a001c87e2c79b2d8a50d38c16cf12f3d6ee',
            '0x765e76b260667084accd3957739558ba412843a7',
            '0xd3c8d372bfcd36c2b452639a7ed6ef7dbfdc56f8',
            '0x703d37fb776a4c905e28f7ff23c73102ce36e08b',
            '0x26c6c7f10e271eef0011d07319622f31d22d139c',
            '0x4b9ca5607f1ff8019c1c6a3c2f0cc8de622d5b82'
        )
        AND event_name IN (
            'AnswerUpdated',
            'IssuanceRatioUpdated',
            'Burn',
            'Mint',
            'Transfer'
        )
        {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                MAX(logs_timestamp)
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
        decoded_flat :current / pow(10,8) AS snx_price
    FROM
        applicable_logs
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
        decoded_flat:current / pow(10, 27) AS sds_price
    FROM
        applicable_logs
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
        wallet_address,
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(10,18) AS "SNX Balance",
        trace_index,
        _inserted_timestamp
    FROM
        applicable_traces
    WHERE
        to_address = '0x5b1b5fea1b99d83ad479df0c222f0492385381dd'
    AND input ILIKE '0x70a08231%'
    AND block_timestamp :: DATE >= '2022-01-01' qualify ROW_NUMBER() over (
        PARTITION BY tx_hash,
        wallet_address
        ORDER BY
            trace_index
        ) = 1
),
snxescrow_balance AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(10,18) AS "Escrowed SNX Balance"
    FROM
        applicable_traces
    WHERE
        to_address IN (
            '0xb671f2210b1f6621a2607ea63e6b2dc3e2464d1f',
            '0xac86855865cbf31c8f9fbb68c749ad5bd72802e3',
            '0xda4ef8520b1a57d7d63f1e249606d1a459698876'
        )
        AND input ILIKE '0x70a08231%'
        AND block_timestamp :: DATE >= '2022-01-01'
),
sds_balance AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(10,18) AS "SDS Balance"
    FROM
        applicable_traces
    WHERE
        to_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        AND input ILIKE '0x70a08231%'
        AND block_timestamp :: DATE >= '2022-01-01' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            wallet_address
            ORDER BY
                trace_index
        ) = 1
),
l1_target_cratios AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        (1 /(decoded_flat :newRatio :: DECIMAL / pow(10,18))) * 100 AS "Target C-Ratio"
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        event_name = 'IssuanceRatioUpdated'
    ORDER BY
        block_timestamp DESC
),
sds_mints_burns AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        decoded_flat :account AS wallet_address,
        origin_from_address,
        (
            CASE
                WHEN event_name = 'Burn' THEN -1 * decoded_flat :amount :: FLOAT / pow(10,18)
                WHEN event_name = 'Mint' THEN decoded_flat :amount :: FLOAT / pow(10,18)
            END
        ) AS "Minted Amount",
        event_index
    FROM
        applicable_logs
    WHERE
    contract_address ='0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
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
                WHEN decoded_flat :to = '0x0000000000000000000000000000000000000000' THEN decoded_flat :from
                WHEN decoded_flat :from = '0x0000000000000000000000000000000000000000' THEN decoded_flat :to
            END
        ) AS wallet_address,
        (
            CASE
                WHEN decoded_flat :to = '0x0000000000000000000000000000000000000000' THEN -1 * decoded_flat :value :: FLOAT / pow(10,18)
                WHEN decoded_flat :from = '0x0000000000000000000000000000000000000000' THEN decoded_flat :value :: FLOAT / pow(10,18)
            END
        ) AS "Minted Amount",
        event_index
    FROM
        applicable_logs
    WHERE
        contract_address = '0x57ab1ec28d129707052df4df418d58a2d46d5f51'
        AND (
            decoded_flat :from = '0x0000000000000000000000000000000000000000'
            OR decoded_flat :to = '0x0000000000000000000000000000000000000000'
        )
        AND block_timestamp :: DATE >= '2022-01-01'
),
snx_transfers AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        decoded_flat :from AS from_address,
        decoded_flat :to AS to_address,
        decoded_flat :value :: FLOAT / pow(10,18) AS trf_value
    FROM
        applicable_logs
    WHERE
        contract_address = '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f'
        AND (
            decoded_flat :from IN (
                SELECT
                    wallet_address
                FROM
                    sds_mints_burns
            )
            OR decoded_flat :to IN (
                SELECT
                    wallet_address
                FROM
                    sds_mints_burns
            )
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
                WHEN bal.wallet_address = trf.to_address THEN trf.trf_value
                WHEN bal.wallet_address = trf.from_address THEN -1 * trf.trf_value
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
        wallet_address,
        "SNX Balance",
        SUM(snx_trf_value) over (
            PARTITION BY tx_hash,
            wallet_address
            ORDER BY
                block_timestamp
        ) AS net_trf_amount
    FROM
        bal_with_snxtrf_raw1 qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            wallet_address
            ORDER BY
                block_timestamp
        ) = 1
),
bal_with_snxtrf AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
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
        bal.wallet_address,
        trf.event_name,
        bal."SDS Balance" AS initial_balance,
        trf."Minted Amount",
        SUM(
            trf."Minted Amount"
        ) over (
            PARTITION BY bal.tx_hash,
            bal.wallet_address
            ORDER BY
                trf.event_index
        ) AS net_sum,
        bal."SDS Balance" + net_sum AS "SDS Balance",
        trf.event_index
    FROM
        sds_balance bal
        JOIN sds_mints_burns trf
        ON bal.tx_hash = trf.tx_hash
        AND bal.wallet_address = trf.wallet_address
),
bal_with_sds_trf AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
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
        wallet_address,
        event_name,
        "Minted Amount",
        "SDS Balance",
        sds_price,
        event_index
    FROM
        add_sdsprice_raw qualify ROW_NUMBER() over (
            PARTITION BY wallet_address,
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
        applicable_logs
    WHERE
        contract_address IN (
            '0x57ab1ec28d129707052df4df418d58a2d46d5f51',
            '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        )
        AND (
            decoded_flat :from IN (
                SELECT
                    wallet_address
                FROM
                    absent_sdsbalances
            )
            OR decoded_flat :to IN (
                SELECT
                    wallet_address
                FROM
                    absent_sdsbalances
            )
        )
        AND (
            decoded_flat :from = '0x0000000000000000000000000000000000000000'
            OR decoded_flat :to = '0x0000000000000000000000000000000000000000'
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
        susd.decoded_flat :value :: DECIMAL / pow(10,18) AS susd_amount,
        sds.decoded_flat :value :: DECIMAL / pow(10,18) AS sds_amount,
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
        sdsbal.wallet_address,
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
        sds.wallet_address,
        snx."SNX Balance",
        snxes."Escrowed SNX Balance",
        sds."SDS Balance",
        sds.sds_price
    FROM
        add_sdsprice sds
        JOIN bal_with_snxtrf snx
        ON sds.tx_hash = snx.tx_hash
        AND sds.wallet_address = snx.wallet_address
        JOIN snxescrow_balance snxes
        ON sds.tx_hash = snxes.tx_hash
        AND sds.wallet_address = snxes.wallet_address qualify ROW_NUMBER() over (
            PARTITION BY sds.tx_hash,
            sds.wallet_address
            ORDER BY
                sds.block_timestamp,
                sds.event_index DESC
        ) = 1
),
imported_address_mints AS (
    SELECT
        *
    FROM
        {{ ref('silver__synthetix_imported_address_mints') }}
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
        ) AS wallet_address,
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(10,18) AS "Escrowed SNX Amount"
    FROM
        applicable_traces
    WHERE
        to_address IN (
            '0xb671f2210b1f6621a2607ea63e6b2dc3e2464d1f',
            '0xac86855865cbf31c8f9fbb68c749ad5bd72802e3',
            '0xda4ef8520b1a57d7d63f1e249606d1a459698876'
        )
        AND input ILIKE '0x70a08231%'
    ORDER BY
        block_timestamp
),
imported_address_SNX_balance_raw AS (
    SELECT
        block_number,
        block_timestamp,
        user_address AS wallet_address,
        current_bal_unadj / pow(10,18) AS "SNX Balance Amount"
    FROM
        {{ ref("core__ez_balance_deltas") }}
    WHERE
        block_timestamp <= '2022-02-09 05:01:00.000'
        AND user_address IN (
            SELECT
                wallet_address
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
        wallet_address IN (
            SELECT
                wallet_address
            FROM
                imported_address_mints
        )
        AND block_timestamp <= '2022-02-09 05:01:00.000' qualify ROW_NUMBER() over (
            PARTITION BY wallet_address
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
        wallet_address NOT IN (
            SELECT
                wallet_address
            FROM
                imported_address_SNX_balance_raw
        )
),
imported_address_SNX_balance_raw1 AS (
    SELECT
        block_number,
        block_timestamp,
        wallet_address,
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
        sds.wallet_address,
        snx."SNX Balance Amount" AS "SNX Balance",
        esc."Escrowed SNX Amount" AS "Escrowed SNX Balance",
        sds.minted_amount AS "SDS Balance"
    FROM
        imported_address_mints sds
        LEFT JOIN imported_address_SNX_balance snx
        ON sds.wallet_address = snx.wallet_address
        LEFT JOIN imported_address_escrow_balance esc
        ON esc.wallet_address = snx.wallet_address
),
imported_combined_balances AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        'Mint' AS event_name,
        "SDS Balance" AS "Minted Amount",
        wallet_address,
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
            wallet_address,
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
        applicable_traces
    ORDER BY
        block_timestamp
    LIMIT
        1
    {% endif %}
), 
imported_combined_balances_sdsprice AS (
    SELECT
        bal.*,
        1.003239484 AS sds_price
    FROM
        imported_combined_balances bal
),
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
            PARTITION BY bal.wallet_address,
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
),
bal_cRatio_join as (
    SELECT
        bal.block_number,
        bal.block_timestamp,
        bal.tx_hash,
        bal.wallet_address,
        concat_ws('',bal.tx_hash,bal.wallet_address) as wallet_tx_hash,
        bal.event_name,
        (bal."Minted Amount" :: FLOAT) as minted_amount,
        bal."SNX Balance" as snx_balance,
        bal."Escrowed SNX Balance" as escrowed_snx_balance,
        bal."SDS Balance" as sds_balance,
        coalesce(SNX_PRICE,5.421) as snx_price,
        coalesce(SDS_PRICE,1.003239484) as sds_price,
        bal."Account C-Ratio" as account_c_ratio,
        cratio."Target C-Ratio" as target_c_ratio
    FROM
        combined_balances_prices_final bal
        JOIN l1_target_cratios cratio
        ON bal.block_timestamp >= cratio.block_timestamp qualify ROW_NUMBER() over (
            PARTITION BY bal.tx_hash,
            bal.wallet_address,
            bal."SDS Balance"
            ORDER BY
                cratio.block_timestamp DESC
        ) = 1
    ORDER BY
        bal.block_timestamp DESC
),
ranked_traces AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tx_hash
            ORDER BY block_timestamp DESC
        ) AS row_num
    FROM
        applicable_traces
),
ranked_logs AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tx_hash
            ORDER BY block_timestamp DESC
        ) AS row_num
    FROM
        applicable_logs
),

SELECT
        bal.block_number,
        bal.block_timestamp,
        bal.tx_hash,
        bal.wallet_address as user_address,
        bal.wallet_tx_hash,
        bal.event_name,
        bal.minted_amount,
        bal.snx_balance,
        bal.escrowed_snx_balance,
        bal.sds_balance,
        bal.snx_price,
        bal.sds_price,
        bal.account_c_ratio,
        bal.target_c_ratio,
        t._inserted_timestamp AS traces_timestamp,
        l._inserted_timestamp AS logs_timestamp
    FROM
        bal_cRatio_join bal
    LEFT JOIN
        ranked_traces t
    ON
        bal.tx_hash = t.tx_hash
        AND t.row_num = 1
    LEFT JOIN
        ranked_logs l
    ON
        bal.tx_hash = l.tx_hash
        AND l.row_num = 1
