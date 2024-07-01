{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH withdraw AS (
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
        CASE 
            WHEN segmented_input [5] :: STRING = '0000000000000000000000000000000000000000000000000000000000000000' 
            THEN utils.udf_hex_to_int(
                segmented_input [6] :: STRING
            ) ::INTEGER
            ELSE  
            utils.udf_hex_to_int(
                segmented_input [5] :: STRING
            ) ::INTEGER
            END 
        AS withdraw_amount,
        CONCAT('0x', SUBSTR(segmented_input [7] :: STRING, 25)) AS on_behalf_address,
        CONCAT('0x', SUBSTR(segmented_input [8] :: STRING, 25)) AS receiver_address,
        _call_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        to_address = '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb' --Morpho Blue
        AND function_sig = '0x5c2bea49'
        AND trace_status = 'SUCCESS' 

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_trace_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
logs_level AS (
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
        topics [1] :: STRING AS market_id,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS onBehalfOf,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS reciever,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS caller,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS withdraw_amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS shares,
        l._inserted_timestamp,
        l._log_id,
        'Morpho Blue' AS morpho_version,
        origin_from_address AS depositor_address,
        contract_address AS lending_pool_contract
    FROM
        silver.logs l
    WHERE
        topics [0] :: STRING = '0xa56fc0ad5702ec05ce63666221f796fb62437c32db1aa1aa075fc6484cf58fbf'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    b.tx_hash,
    b.block_number,
    b.block_timestamp,
    l.event_index,
    l.origin_from_address,
    l.origin_to_address,
    l.origin_function_signature,
    l.contract_address,
    b.loan_token AS market,
    b.withdraw_amount AS amount_unadj,
    b.withdraw_amount / pow(
        10,
        C.decimals
    ) AS amount,
    l.depositor_address,
    l.lending_pool_contract,
    C.symbol,
    C.decimals,
    l.morpho_version as platform,
    'ethereum' as blockchain,
    l._inserted_timestamp,
    l._log_id,
    b._call_id,
    b._inserted_timestamp AS _inserted_trace_timestamp
FROM
    withdraw b
    LEFT JOIN logs_level l
    ON l.tx_hash = b.tx_hash
    AND l.withdraw_amount = b.withdraw_amount
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON address = b.loan_token
