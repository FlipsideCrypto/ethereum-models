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
        decoded_data:decoded_input_data:loanToken::STRING AS loan_token,
        decoded_data:decoded_input_data:marketParams:collateralToken::STRING AS collateral_token,
        decoded_data:decoded_input_data:marketParams:oracle::STRING AS oracle_address,
        decoded_data:decoded_input_data:marketParams:irm::STRING AS irm_address,
        decoded_data:decoded_input_data:borrower::STRING AS borrower,
        decoded_data:decoded_output_data:output_1::INTEGER AS amount,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                type,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver__decoded_traces') }}
    WHERE
        to_address = '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb' --Morpho Blue
        AND function_sig = '0xd8eabcb8'
        AND trace_succeeded
        AND tx_succeeded

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
),
logs AS(
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.event_index,
        l.origin_from_address,
        l.origin_to_address,
        l.origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS caller,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS repay_assets,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS seized_assets,
        COALESCE(
            l.origin_to_address,
            l.contract_address
        ) AS lending_pool_contract,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        topics [0] :: STRING = '0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41'
        AND l.contract_address = '0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                traces
        )
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_index,
    l.origin_from_address,
    l.origin_to_address,
    l.origin_function_signature,
    l.contract_address,
    l.caller AS liquidator,
    l.borrower,
    t.loan_token AS debt_asset,
    c0.symbol AS debt_asset_symbol,
    l.repay_assets AS repayed_amount_unadj,
    l.repay_assets / pow(
        10,
        c0.decimals
    ) AS repayed_amount,
    t.collateral_token AS collateral_asset,
    c1.symbol AS collateral_asset_symbol,
    l.seized_assets AS amount_unadj,
    l.seized_assets / pow(
        10,
        c1.decimals
    ) AS amount,
    'Morpho Blue' AS platform,
    'ethereum' AS blockchain,
    t._call_id AS _id,
    t._inserted_timestamp
FROM
    traces t
    INNER JOIN logs l
    ON l.tx_hash = t.tx_hash
    AND l.seized_assets = t.amount
    LEFT JOIN {{ ref('silver__contracts') }}
    c0
    ON c0.address = t.loan_token
    LEFT JOIN {{ ref('silver__contracts') }}
    c1
    ON c1.address = t.collateral_token