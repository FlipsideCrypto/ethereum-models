{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform','version'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
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
        'v1' AS version,
        sender,
        receiver,
        destination_chain_id,
        NULL AS destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__across_fundsdeposited') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
        'v1' AS version,
        sender,
        receiver,
        NULL AS destination_chain_id,
        destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__allbridge_sent') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
axelar_squid AS (
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
        'v1' AS version,
        sender,
        receiver,
        NULL AS destination_chain_id,
        destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__axelar_squid_contractcallwithtoken') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
        'v1' AS version,
        sender,
        receiver,
        destination_chain_id,
        NULL AS destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__celer_cbridge_send') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
        'v1' AS version,
        sender,
        receiver,
        destination_chain_id,
        NULL AS destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__hop_transfersenttol2') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
        'v1' AS version,
        sender,
        receiver,
        destination_chain_id,
        NULL AS destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__multichain_v7_loganyswapout') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
        'v1' AS version,
        sender,
        receiver,
        destination_chain_id,
        NULL AS destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__symbiosis_synthesizerequest') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
synapse_td AS (
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
        'v1-td' AS version,
        sender,
        receiver,
        destination_chain_id,
        NULL AS destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__synapse_tokendeposit') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
synapse_tds AS (
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
        'v1-tds' AS version,
        sender,
        receiver,
        destination_chain_id,
        NULL AS destination_chain,
        token_address,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__synapse_tokendepositandswap') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
native_bridges AS (
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
        'v1' AS version,
        sender,
        receiver,
        NULL AS destination_chain_id,
        destination_chain,
        token_address,
        amount_unadj,
        _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__native_bridges_transfers_out') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
all_bridges AS (
    SELECT
        *
    FROM
        across
    UNION ALL
    SELECT
        *
    FROM
        allbridge
    UNION ALL
    SELECT
        *
    FROM
        celer_cbridge
    UNION ALL
    SELECT
        *
    FROM
        hop
    UNION ALL
    SELECT
        *
    FROM
        multichain
    UNION ALL
    SELECT
        *
    FROM
        symbiosis
    UNION ALL
    SELECT
        *
    FROM
        synapse_td
    UNION ALL
    SELECT
        *
    FROM
        synapse_tds
    UNION ALL
    SELECT
        *
    FROM
        native_bridges
),
FINAL AS (
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
        version,
        sender,
        receiver,
        CASE
            WHEN destination_chain_id IS NULL THEN d.chain_id :: STRING
            ELSE destination_chain_id :: STRING
        END AS destination_chain_id,
        CASE
            WHEN destination_chain IS NULL THEN LOWER(
                d.chain
            )
            ELSE destination_chain
        END AS destination_chain,
        b.token_address,
        symbol AS token_symbol,
        amount_unadj,
        CASE
            WHEN decimals IS NULL THEN amount_unadj
            ELSE (amount_unadj / pow(10, decimals))
        END AS amount,
        CASE
            WHEN decimals IS NOT NULL THEN ROUND(
                amount * p.price,
                2
            )
            ELSE NULL
        END AS amount_usd,
        _id,
        _inserted_timestamp
    FROM
        all_bridges b
        LEFT JOIN contracts C
        ON b.token_address = C.address
        LEFT JOIN prices p
        ON b.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ source(
            'external_gold_defillama',
            'dim_chains'
        ) }}
        d
        ON d.chain_id :: STRING = b.destination_chain_id :: STRING
        OR LOWER(
            d.chain
        ) = LOWER(
            b.destination_chain
        )
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
    version,
    sender,
    receiver,
    destination_chain_id,
    destination_chain,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    _id,
    _inserted_timestamp
FROM
    FINAL --destination_chain --name of chain, join with defillama chains table
    --convert chain symbol to id for allbridge
    --verify all chains have ids and vice versa
