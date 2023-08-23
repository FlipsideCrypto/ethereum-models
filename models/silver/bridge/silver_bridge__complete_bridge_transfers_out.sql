{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH contracts AS (

    SELECT
        address,
        symbol,
        decimals
    FROM
        {{ ref('silver__contracts') }}
),
prices AS (
    SELECT
        HOUR,
        token_address,
        price
    FROM
        {{ ref('core__fact_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT address
            FROM
                contracts
        )

{% if is_incremental() %}
AND HOUR >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
across AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__across_fundsdeposited') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
allbridge AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__allbridge_sent') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
celer_cbridge AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__celer_cbridge_send') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
hop AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__hop_transfersenttol2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
multichain AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__multichain_v7_loganyswapout') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
symbiosis AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__symbiosis_synthesizerequest') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
synapse AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__synapse_tokendeposit') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__synapse_tokendepositandswap') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
native_bridges AS (--determine structure for native assets from traces
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        bridge_name AS platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount_unadj
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__native_bridges_transfers_out') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
all_bridges AS (
    SELECT *
    FROM across
    UNION ALL
    SELECT *
    FROM allbridge
    UNION ALL
    SELECT *
    FROM celer_cbridge
    UNION ALL
    SELECT *
    FROM hop
    UNION ALL
    SELECT *
    FROM multichain
    UNION ALL
    SELECT *
    FROM symbiosis
    UNION ALL
    SELECT *
    FROM synapse
    UNION ALL
    FROM native_bridges
)

SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    bridge_address,
    event_name,
    platform,
    sender,
    receiver,
    destination_chain_id,
    --destination_chain,
    b.token_address,
    symbol AS token_symbol,
    amount_unadj,
    CASE
      WHEN decimals IS NULL THEN amount_unadj
      ELSE (amount_unadj / pow(10, decimals))
    END AS amount,
    CASE
      WHEN decimals IS NOT NULL THEN ROUND(amount * p.price,2)
      ELSE NULL
    END AS amount_usd,
    _log_id,
    _inserted_timestamp
FROM all_bridges b
LEFT JOIN contracts c 
    ON b.token_address = c.address
LEFT JOIN prices p 
    ON b.token_address = p.token_address 
        AND DATE_TRUNC('hour',b.block_timestamp) = p.hour 


--token_symbol, --join with contracts for symbol
--amount_adj --join with contracts for decimals
--amount_usd --join with prices for price
--destination_chain --name of chain, join with defillama chains table
--convert chain symbol to id for allbridge