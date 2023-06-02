{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH deposits AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.tx_hash,
        event_index,
        l.contract_address,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS l_from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS token_amount,
        (token_amount / pow(10, 18)) :: FLOAT AS token_amount_adj,
        t.eth_value * pow(
            10,
            18
        ) AS eth_amount,
        t.eth_value AS eth_amount_adj,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__traces') }}
        t
        ON l.tx_hash = t.tx_hash
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) = t.from_address
    WHERE
        topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Deposit/Mint (Transfer)
        AND l.contract_address = '0x3802c218221390025bceabbad5d8c59f40eb74b8' --Guarded Ether (GETH)
        AND l_from_address = '0x0000000000000000000000000000000000000000'
        AND origin_to_address = '0x384365838b002f60544ece69153d685c8f5306e5'

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address,
    l_from_address AS sender,
    to_address AS recipient,
    eth_amount,
    eth_amount_adj,
    token_amount,
    token_amount_adj,
    _log_id,
    _inserted_timestamp
FROM
    deposits
