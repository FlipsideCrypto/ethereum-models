{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
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
            OR input ILIKE '0xc2f04b0a%'
            OR input ILIKE '0x70a08231%'
            OR input ILIKE '0x8f849518%'
        )
        AND to_address IN (
            '0xb671f2210b1f6621a2607ea63e6b2dc3e2464d1f',
            '0xac86855865cbf31c8f9fbb68c749ad5bd72802e3',
            '0xda4ef8520b1a57d7d63f1e249606d1a459698876',
            '0x5b1b5fea1b99d83ad479df0c222f0492385381dd',
            '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        )
        AND tx_status = 'SUCCESS'
        AND block_timestamp :: DATE >= '2022-01-01'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(traces_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
applicable_logs AS (
    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        transformed,
        _LOG_ID,
        _INSERTED_TIMESTAMP,
        decoded_flat,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        DATA,
        event_removed,
        tx_status,
        is_pending
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
        MAX(logs_timestamp) :: DATE
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
        decoded_flat :current / pow(
            10,
            8
        ) AS snx_price
    FROM
        applicable_logs
    WHERE
        contract_address = '0x06ce8be8729b6ba18dd3416e3c223a5d4db5e755'
        AND event_name = 'AnswerUpdated'
        AND block_timestamp :: DATE >= '2022-01-01'
),
sds_price_feeds AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        decoded_flat :current / pow(
            10,
            27
        ) AS sds_price
    FROM
        applicable_logs
    WHERE
        contract_address = '0xc7bb32a4951600fbac701589c73e219b26ca2dfc'
        AND event_name = 'AnswerUpdated'
        AND block_timestamp :: DATE >= '2022-01-01'
),
snx_balance AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(
            10,
            18
        ) AS snx_balance,
        trace_index,
        _inserted_timestamp
    FROM
        applicable_traces
    WHERE
        to_address = '0x5b1b5fea1b99d83ad479df0c222f0492385381dd'
        AND input ILIKE '0x70a08231%'
        AND block_timestamp :: DATE >= '2022-01-01' qualify (ROW_NUMBER() over (PARTITION BY tx_hash, wallet_address
    ORDER BY
        trace_index)) = 1
),
snxescrow_balance AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(
            10,
            18
        ) AS escrowed_snx_balance
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
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(
            10,
            18
        ) AS sds_balance
    FROM
        applicable_traces
    WHERE
        to_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        AND input ILIKE '0x70a08231%'
        AND block_timestamp :: DATE >= '2022-01-01' qualify (ROW_NUMBER() over (PARTITION BY tx_hash, wallet_address
    ORDER BY
        trace_index DESC)) = 1
),
l1_target_cratios AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        (
            1 /(decoded_flat :newRatio :: DECIMAL / pow(10, 18))
        ) * 100 AS target_c_ratio
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        event_name = 'IssuanceRatioUpdated'
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
                WHEN event_name = 'Burn' THEN -1 * decoded_flat :amount :: FLOAT / pow(
                    10,
                    18
                )
                WHEN event_name = 'Mint' THEN decoded_flat :amount :: FLOAT / pow(
                    10,
                    18
                )
            END
        ) AS minted_amount,
        event_index
    FROM
        applicable_logs
    WHERE
        contract_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
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
                WHEN decoded_flat :to = '0x0000000000000000000000000000000000000000' THEN -1 * decoded_flat :value :: FLOAT / pow(
                    10,
                    18
                )
                WHEN decoded_flat :from = '0x0000000000000000000000000000000000000000' THEN decoded_flat :value :: FLOAT / pow(
                    10,
                    18
                )
            END
        ) AS minted_amount,
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
        decoded_flat :value :: FLOAT / pow(
            10,
            18
        ) AS trf_value
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
        bal.tx_hash,
        bal.block_number,
        bal.block_timestamp,
        bal.wallet_address,
        bal.snx_balance,
        bal.trace_index,
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
        snx_balance,
        SUM(snx_trf_value) over (
            PARTITION BY tx_hash,
            wallet_address
            ORDER BY
                block_timestamp
        ) AS net_trf_amount
    FROM
        bal_with_snxtrf_raw1 qualify (ROW_NUMBER() over (PARTITION BY tx_hash, wallet_address
    ORDER BY
        block_timestamp)) = 1
),
bal_with_snxtrf AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        (
            CASE
                WHEN snx_balance + net_trf_amount < 0 THEN 0
                ELSE snx_balance + net_trf_amount
            END
        ) AS snx_balance
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
        bal.sds_balance AS initial_balance,
        trf.minted_amount,
        SUM(
            trf.minted_amount
        ) over (
            PARTITION BY bal.tx_hash,
            bal.wallet_address
            ORDER BY
                trf.event_index
        ) AS net_sum,
        bal.sds_balance + net_sum AS sds_balance,
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
        minted_amount,
        sds_balance,
        event_index
    FROM
        bal_with_sds_trf_raw
),
add_sdsprice_raw AS (
    SELECT
        bal.tx_hash,
        bal.block_number,
        bal.block_timestamp,
        bal.wallet_address,
        bal.event_name,
        bal.minted_amount,
        bal.sds_balance,
        bal.event_index,
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
        minted_amount,
        sds_balance,
        sds_price,
        event_index
    FROM
        add_sdsprice_raw qualify (ROW_NUMBER() over (PARTITION BY wallet_address, tx_hash, minted_amount, event_name
    ORDER BY
        sdsprice_timestamp DESC)) = 1
),
absent_sdsbalances AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        event_name,
        minted_amount,
        sds_balance,
        event_index
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
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        transformed,
        _LOG_ID,
        _INSERTED_TIMESTAMP,
        decoded_flat,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        DATA,
        event_removed,
        tx_status,
        is_pending
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
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        transformed,
        _LOG_ID,
        _INSERTED_TIMESTAMP,
        decoded_flat,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        DATA,
        event_removed,
        tx_status,
        is_pending
    FROM
        absent_tx_eventlogs
    WHERE
        contract_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f' qualify (ROW_NUMBER() over (PARTITION BY tx_hash
    ORDER BY
        event_index DESC)) = 1
),
absent_tx_susd AS (
    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        transformed,
        _LOG_ID,
        _INSERTED_TIMESTAMP,
        decoded_flat,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        DATA,
        event_removed,
        tx_status,
        is_pending
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
        susd.decoded_flat :value :: DECIMAL / pow(
            10,
            18
        ) AS susd_amount,
        sds.decoded_flat :value :: DECIMAL / pow(
            10,
            18
        ) AS sds_amount,
        susd_amount / sds_amount AS sds_price
    FROM
        absent_tx_sds sds
        JOIN absent_tx_susd susd
        ON sds.tx_hash = susd.tx_hash
        AND sds.event_index < susd.event_index
    WHERE
        sds_amount != 0 qualify (ROW_NUMBER() over (PARTITION BY sds.tx_hash
    ORDER BY
        susd.event_index)) = 1
),
add_sdsprice2 AS (
    SELECT
        sdsbal.tx_hash,
        sdsbal.block_number,
        sdsbal.block_timestamp,
        sdsbal.wallet_address,
        sdsbal.event_name,
        sdsbal.minted_amount,
        sdsbal.sds_balance,
        sdsprice.sds_price,
        sdsbal.event_index
    FROM
        absent_sdsbalances sdsbal
        JOIN absent_sdsprices_data sdsprice
        ON sdsbal.tx_hash = sdsprice.tx_hash
),
add_sdsprice AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        event_name,
        minted_amount,
        sds_balance,
        sds_price,
        event_index
    FROM
        add_sdsprice1
    UNION
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        event_name,
        minted_amount,
        sds_balance,
        sds_price,
        event_index
    FROM
        add_sdsprice2
),
combined_balances_sdsprice_raw AS (
    SELECT
        sds.block_number,
        sds.block_timestamp,
        sds.tx_hash,
        sds.event_name,
        sds.minted_amount,
        sds.wallet_address,
        snx.snx_balance,
        snxes.escrowed_snx_balance,
        sds.sds_balance,
        sds.sds_price
    FROM
        add_sdsprice sds
        JOIN bal_with_snxtrf snx
        ON sds.tx_hash = snx.tx_hash
        AND sds.wallet_address = snx.wallet_address
        JOIN snxescrow_balance snxes
        ON sds.tx_hash = snxes.tx_hash
        AND sds.wallet_address = snxes.wallet_address qualify (ROW_NUMBER() over (PARTITION BY sds.tx_hash, sds.wallet_address
    ORDER BY
        sds.block_timestamp, sds.event_index DESC)) = 1
),
imported_address_traces AS (
    SELECT
        block_timestamp,
        tx_hash,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        input ILIKE '0x8f849518%'
        AND block_timestamp :: DATE = '2022-02-09'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(traces_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
imported_address_mints AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.event_index,
        decoded_flat :account AS wallet_address,
        concat_ws(
            '',
            l.tx_hash,
            l.decoded_flat :account
        ) AS wallet_tx_hash,
        CAST(
            decoded_flat :amount AS DECIMAL
        ) / pow(
            10,
            18
        ) AS minted_amount
    FROM
        {{ ref('silver__decoded_logs') }}
        l
    WHERE
        l.tx_hash IN (
            SELECT
                tx_hash
            FROM
                imported_address_traces
        )
        AND tx_status = 'SUCCESS'
        AND event_name = 'Mint'
        AND block_timestamp :: DATE = '2022-02-09'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(logs_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
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
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(
            10,
            18
        ) AS escrowed_snx_balance
    FROM
        applicable_traces
    WHERE
        to_address IN (
            '0xb671f2210b1f6621a2607ea63e6b2dc3e2464d1f',
            '0xac86855865cbf31c8f9fbb68c749ad5bd72802e3',
            '0xda4ef8520b1a57d7d63f1e249606d1a459698876'
        )
        AND input ILIKE '0x70a08231%'
),
imported_address_SNX_balance_raw AS (
    SELECT
        block_number,
        block_timestamp,
        user_address AS wallet_address,
        current_bal_unadj / pow(
            10,
            18
        ) AS snx_balance
    FROM
        {{ ref('core__ez_balance_deltas') }}
    WHERE
        block_timestamp <= '2022-02-09 05:01:00.000'
        AND user_address IN (
            SELECT
                wallet_address
            FROM
                imported_address_mints
        )
        AND (symbol IN ('SNX', 'HAV')
        OR contract_address = '0xc011a72400e58ecd99ee497cf89e3775d4bd732f') qualify (ROW_NUMBER() over (PARTITION BY user_address
    ORDER BY
        block_timestamp DESC)) = 1
),
imported_address_escrow_balance AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        wallet_address,
        escrowed_snx_balance
    FROM
        imported_address_escrow_balance_raw
    WHERE
        wallet_address IN (
            SELECT
                wallet_address
            FROM
                imported_address_mints
        )
        AND block_timestamp <= '2022-02-09 05:01:00.000' qualify (ROW_NUMBER() over (PARTITION BY wallet_address
    ORDER BY
        block_timestamp DESC)) = 1
),
absent_addresses_snx AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        wallet_address,
        minted_amount
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
        0 AS snx_balance
    FROM
        absent_addresses_snx
),
imported_address_SNX_balance AS (
    SELECT
        block_number,
        block_timestamp,
        wallet_address,
        snx_balance
    FROM
        imported_address_SNX_balance_raw
    UNION
    SELECT
        block_number,
        block_timestamp,
        wallet_address,
        snx_balance
    FROM
        imported_address_SNX_balance_raw1
),
imported_combined_balances_raw AS (
    SELECT
        sds.block_number,
        sds.block_timestamp,
        sds.tx_hash,
        sds.wallet_address,
        snx.snx_balance,
        esc.escrowed_snx_balance,
        sds.minted_amount AS sds_balance
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
        sds_balance AS minted_amount,
        wallet_address,
        snx_balance,
        (
            CASE
                WHEN escrowed_snx_balance IS NULL THEN 0
                ELSE escrowed_snx_balance
            END
        ) AS escrowed_snx_balance,
        sds_balance
    FROM
        imported_combined_balances_raw
),
earliest_burn_mintshare_txn AS (

{% if is_incremental() %}
SELECT
    tx_hash, user_address, block_timestamp
FROM
    {{ this }}
ORDER BY
    block_timestamp
LIMIT
    1
{% else %}
SELECT
    tx_hash, to_address, block_number, block_timestamp, wallet_address, input, output, trace_index
FROM
    applicable_traces
ORDER BY
    block_timestamp
LIMIT
    1
{% endif %}), imported_combined_balances_sdsprice AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        minted_amount,
        wallet_address,
        snx_balance,
        escrowed_snx_balance,
        sds_balance,
        1.003239484 AS sds_price
    FROM
        imported_combined_balances bal
),
combined_balances_sdsprice AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        minted_amount,
        wallet_address,
        snx_balance,
        escrowed_snx_balance,
        sds_balance,
        sds_price
    FROM
        combined_balances_sdsprice_raw
    UNION
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        minted_amount,
        wallet_address,
        snx_balance,
        escrowed_snx_balance,
        sds_balance,
        sds_price
    FROM
        imported_combined_balances_sdsprice
),
combined_balances_prices AS (
    SELECT
        bal.block_number,
        bal.block_timestamp,
        bal.tx_hash,
        bal.event_name,
        bal.minted_amount,
        bal.wallet_address,
        bal.snx_balance,
        bal.escrowed_snx_balance,
        bal.sds_balance,
        bal.sds_price,
        snxprice.snx_price
    FROM
        combined_balances_sdsprice bal
        JOIN snx_price_feeds snxprice
        ON bal.block_timestamp >= snxprice.block_timestamp qualify (ROW_NUMBER() over (PARTITION BY bal.wallet_address, bal.tx_hash
    ORDER BY
        snxprice.block_timestamp DESC)) = 1
),
combined_balances_prices_final AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_name,
        minted_amount,
        wallet_address,
        snx_balance,
        escrowed_snx_balance,
        sds_balance,
        sds_price,
        snx_price,
        (
            CASE
                WHEN sds_balance = 0 THEN 0
                ELSE (
                    ((snx_balance + escrowed_snx_balance) * snx_price) /(
                        sds_balance * sds_price
                    )
                ) * 100
            END
        ) AS account_c_ratio
    FROM
        combined_balances_prices
),
bal_cRatio_join AS (
    SELECT
        bal.block_number,
        bal.block_timestamp,
        bal.tx_hash,
        bal.wallet_address,
        concat_ws(
            '',
            bal.tx_hash,
            bal.wallet_address
        ) AS wallet_tx_hash,
        bal.event_name,
        (
            bal.minted_amount :: FLOAT
        ) AS minted_amount,
        bal.snx_balance AS snx_balance,
        bal.escrowed_snx_balance,
        bal.sds_balance,
        COALESCE(
            snx_price,
            5.421
        ) AS snx_price,
        COALESCE(
            sds_price,
            1.003239484
        ) AS sds_price,
        bal.account_c_ratio AS account_c_ratio,
        cratio.target_c_ratio AS target_c_ratio
    FROM
        combined_balances_prices_final bal
        JOIN l1_target_cratios cratio
        ON bal.block_timestamp >= cratio.block_timestamp qualify (ROW_NUMBER() over (PARTITION BY bal.tx_hash, bal.wallet_address, bal.escrowed_snx_balance
    ORDER BY
        cratio.block_timestamp DESC)) = 1
)
SELECT
    bal.block_number,
    bal.block_timestamp,
    bal.tx_hash,
    bal.wallet_address AS user_address,
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
    LEFT JOIN applicable_traces t
    ON bal.tx_hash = t.tx_hash
    LEFT JOIN applicable_logs l
    ON bal.tx_hash = l.tx_hash qualify (ROW_NUMBER() over (PARTITION BY wallet_tx_hash
ORDER BY
    GREATEST(traces_timestamp, logs_timestamp) DESC)) = 1
