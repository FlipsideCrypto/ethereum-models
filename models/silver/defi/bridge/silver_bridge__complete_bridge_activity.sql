{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform','version'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH across AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
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
        origin_function_signature,
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
        origin_function_signature,
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
        origin_function_signature,
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
        origin_function_signature,
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
        origin_function_signature,
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
        origin_function_signature,
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
        origin_function_signature,
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
        origin_function_signature,
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
all_protocols AS (
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
        axelar_squid
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
),
native_bridges AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        bridge_name AS platform,
        'v1-native' AS version,
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
    WHERE
        tx_hash NOT IN (
            SELECT
                DISTINCT tx_hash
            FROM
                all_protocols
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
        all_protocols
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
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        version,
        sender,
        receiver,
        CASE
            WHEN d.chain_id IS NULL THEN destination_chain_id :: STRING
            ELSE d.chain_id :: STRING
        END AS destination_chain_id,
        CASE
            WHEN d.chain IS NULL THEN LOWER(destination_chain)
            ELSE LOWER(
                d.chain
            )
        END AS destination_chain,
        b.token_address,
        C.symbol AS token_symbol,
        C.decimals AS token_decimals,
        amount_unadj,
        CASE
            WHEN C.decimals IS NOT NULL THEN (amount_unadj / pow(10, C.decimals))
            ELSE amount_unadj
        END AS amount,
        CASE
            WHEN C.decimals IS NOT NULL THEN ROUND(
                amount * p.price,
                2
            )
            ELSE NULL
        END AS amount_usd,
        _id,
        b._inserted_timestamp
    FROM
        all_bridges b
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON b.token_address = C.address
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
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
    origin_function_signature,
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
    token_decimals,
    amount_unadj,
    amount,
    CASE
        WHEN amount_usd < 1e+17 THEN amount_usd
        ELSE NULL
    END AS amount_usd,
    _id,
    _inserted_timestamp
FROM
    FINAL
