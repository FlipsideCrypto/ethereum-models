{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH traces AS (

    SELECT
        block_number,
        tx_hash,
        block_timestamp,
        from_address,
        to_address,
        LEFT(
            input,
            10
        ) AS function_sig,
        regexp_substr_all(SUBSTR(input, 11), '.{64}') AS segmented_input,
        CONCAT('0x', SUBSTR(segmented_input [0] :: STRING, 25)) AS loan_token,
        CONCAT('0x', SUBSTR(segmented_input [1] :: STRING, 25)) AS collateral_token,
        CONCAT('0x', SUBSTR(segmented_input [2] :: STRING, 25)) AS oracle_address,
        CONCAT('0x', SUBSTR(segmented_input [3] :: STRING, 25)) AS irm_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [4] :: STRING
            )
        ) AS lltv,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [5] :: STRING
            )
        ) AS amount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_input [6] :: STRING
            )
        ) AS shares,
        CONCAT('0x', SUBSTR(segmented_input [7] :: STRING, 25)) AS on_behalf_address,
        CONCAT('0x', SUBSTR(segmented_input [8] :: STRING, 25)) AS receiver_address,
        _call_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
        t
    WHERE
        to_address = '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb' --Morpho Blue
        AND function_sig = '0xa99aad89'
        AND trace_status = 'SUCCESS'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= CURRENT_DATE() - INTERVAL '7 day'
{% endif %}
),
tx_join AS (
    SELECT
        tx.block_number,
        tx.tx_hash,
        tx.block_timestamp,
        tx.from_address as origin_from_address,
        tx.to_address as origin_to_address,
        tx.origin_function_signature,
        t.from_address,
        t.to_address as contract_address,
        tx.from_address as depositor_address,
        t.loan_token,
        t.collateral_token,
        t.amount,
        t.on_behalf_address,
        t.receiver_address,
        t._call_id,
        t._inserted_timestamp
    FROM
        traces t
        INNER JOIN {{ ref('silver__transactions') }}
        tx
        ON tx.block_number = t.block_number
        AND tx.tx_hash = t.tx_hash
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    loan_token AS market,
    amount AS amount_unadj,
    amount / pow(10, C.decimals) AS amount,
    C.symbol,
    C.decimals,
    depositor_address,
    contract_address as lending_pool_contract,
    'Morpho Blue' AS platform,
    'ethereum' AS blockchain,
    _call_id as _id,
    t._inserted_timestamp
FROM
    tx_join  t
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON address = t.loan_token 
