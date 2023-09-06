{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime'],
) }}

with repayments as ( 
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS asset,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS repay_addres,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS destination_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS usd_value,
        origin_from_address AS depositor_address,
        'Compound V3' AS compound_version,
        NAME,
        symbol,
        decimals,
        'ethereum' AS blockchain,
        _log_id,
        l._inserted_timestamp
        from   {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON asset = C.address
    where topics[0] = '0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e' --Supply
-- contract_address IN (
--             '0xa17581a9e3356d9a858b789d68b4d866e593ae94',
--             '0xc3d688b66703497daa19211eedff47f25384cdc3'
--         )
),
prices AS (
    SELECT
        HOUR AS block_hour,
        token_address AS asset,
        AVG(price) AS token_price
    FROM
        ethereum_dev.silver.prices
        INNER JOIN ethereum_dev.silver.contracts
        ON token_address = address
    WHERE
        HOUR :: DATE IN (
            SELECT
                block_timestamp :: DATE
            FROM
                repayments
        )
    GROUP BY
        1,
        2
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    --compound_market,
    w.asset,
    amount / pow(
        10,
        w.decimals
    ) AS supply_tokens,
    amount * token_price / pow(
        10,
        w.decimals
    ) AS supply_usd,
    depositor_address,
    compound_version,
    w.name,
    w.symbol,
    w.decimals,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    repayments w
    LEFT JOIN prices p
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = block_hour
    AND w.asset = p.asset qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
