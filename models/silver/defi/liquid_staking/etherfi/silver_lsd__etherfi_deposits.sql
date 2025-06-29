{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','liquid_staking','curated']
) }}

WITH deposit_logs AS (

    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        'Transfer' AS event_name,
        contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS token_amount,
        (token_amount / pow(10, 18)) :: FLOAT AS token_amount_adj,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp :: DATE >= '2023-07-01'
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Deposit/Mint (Transfer)
        AND contract_address = LOWER('0x35fA164735182de50811E8e2E824cFb9B6118ac2') --ether.fi: eETH Token (eETH)
        AND from_address = '0x0000000000000000000000000000000000000000'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
deposit_traces AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        VALUE * pow(
            10,
            18
        ) AS eth_amount,
        VALUE AS eth_amount_adj,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp :: DATE >= '2023-07-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                deposit_logs
        )
        AND from_address IN (
            SELECT
                to_address
            FROM
                deposit_logs
        )
        AND tx_succeeded
        AND trace_succeeded
        {% if is_incremental() %}
        AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
        {% endif %}
        
)
SELECT
    l.block_number,
    l.block_timestamp,
    l.origin_function_signature,
    l.origin_from_address,
    l.origin_to_address,
    l.tx_hash,
    l.event_index,
    l.event_name,
    l.contract_address,
    l.to_address AS sender,
    l.to_address AS recipient,
    COALESCE(
        eth_amount,
        token_amount
    ) AS eth_amount,
    COALESCE(
        eth_amount_adj,
        token_amount_adj
    ) AS eth_amount_adj,
    token_amount,
    token_amount_adj,
    LOWER(
        l.contract_address
    ) AS token_address,
    'eETH' AS token_symbol,
    'etherfi' AS platform,
    _log_id,
    l._inserted_timestamp
FROM
    deposit_logs l
    LEFT JOIN deposit_traces t
    ON l.tx_hash = t.tx_hash
    AND t.from_address = l.to_address qualify(ROW_NUMBER() over (PARTITION BY _log_id
ORDER BY
    l._inserted_timestamp DESC, eth_amount DESC)) = 1
