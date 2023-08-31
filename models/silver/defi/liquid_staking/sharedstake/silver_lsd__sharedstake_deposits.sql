{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
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
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Deposit/Mint (Transfer)
        AND contract_address = '0x898bad2774eb97cf6b94605677f43b41871410b1' --validator-Eth2 (vETH2)
        AND from_address = '0x0000000000000000000000000000000000000000'
        AND origin_to_address = '0xbca3b7b87dcb15f0efa66136bc0e4684a3e5da4d'

{% if is_incremental() %}
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
deposit_traces AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address,
        to_address,
        eth_value * pow(
            10,
            18
        ) AS eth_amount,
        eth_value AS eth_amount_adj,
        _call_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp :: DATE >= '2020-12-10'
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
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    l.contract_address AS token_address,
    'vETH2' AS token_symbol,
    'sharedstake' AS platform,
    _log_id,
    l._inserted_timestamp
FROM
    deposit_logs l
    LEFT JOIN deposit_traces t
    ON l.tx_hash = t.tx_hash
    AND t.from_address = l.to_address
