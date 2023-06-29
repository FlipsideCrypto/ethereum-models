{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    cluster_by = ['block_timestamp::DATE']
) }}
--only using this for tx_hash where cause filter and one earliest equery, used for a lot of the CTE's tho some incremental here should work its way down. 
--Will need to updated the earlierst query though, can probably use incremental logic there so just pull from model
with applicable_traces as (
    
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
        ETHEREUM_DEV.silver.traces
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
        AND block_timestamp :: DATE >= '2023-04-01' --set this to April 23 for testing, original was 1/1/2022

        
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
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER / pow(10,18) AS "SNX Balance",
        trace_index
    FROM
        applicable_traces
    WHERE
        to_address = '0x5b1b5fea1b99d83ad479df0c222f0492385381dd'
        AND input ILIKE '0x70a08231%'
        AND block_timestamp :: DATE >= '2023-04-01' qualify ROW_NUMBER() over (
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
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER * 1e-18 AS "Escrowed SNX Balance"
    FROM
        applicable_traces
    WHERE
        to_address IN (
            '0xb671f2210b1f6621a2607ea63e6b2dc3e2464d1f',
            '0xac86855865cbf31c8f9fbb68c749ad5bd72802e3',
            '0xda4ef8520b1a57d7d63f1e249606d1a459698876'
        )
        AND input ILIKE '0x70a08231%'
        AND block_timestamp :: DATE >= '2023-04-01'
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
        utils.udf_hex_to_int(SUBSTR(output, 3, 64)) :: INTEGER * 1e-18 AS "SDS Balance"
    FROM
        applicable_traces
    WHERE
        to_address = '0x89fcb32f29e509cc42d0c8b6f058c993013a843f'
        AND input ILIKE '0x70a08231%'
        AND block_timestamp :: DATE >= '2023-04-01' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            "Wallet Address"
            ORDER BY
                trace_index
        ) = 1
)
