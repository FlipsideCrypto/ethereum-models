-- depends_on: {{ ref('silver__complete_token_prices') }}
-- depends_on: {{ ref('price__ez_asset_metadata') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform','version'],
    cluster_by = ['block_timestamp::DATE','platform'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, bridge_address, sender, receiver, destination_chain_receiver, destination_chain_id, destination_chain, token_address, token_symbol), SUBSTRING(origin_function_signature, bridge_address, sender, receiver, destination_chain_receiver, destination_chain, token_address, token_symbol)",
    tags = ['silver_bridge','defi','bridge','curated','heal']
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__across_fundsdeposited') }}

{% if is_incremental() and 'across' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
across_v3 AS (
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
        'v3' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__across_v3fundsdeposited') }}

{% if is_incremental() and 'across_v3' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
        destination_chain_receiver,
        NULL AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__allbridge_sent') }}

{% if is_incremental() and 'allbridge' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
allbridge_v2 AS (
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
        'v2' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__allbridge_tokens_sent') }}

{% if is_incremental() and 'allbridge_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
axie_infinity_v2 AS (
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
        'v2' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__axie_infinity_depositrequested') }}

{% if is_incremental() and 'axie_infinity_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
axelar AS (
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
        destination_chain_receiver,
        NULL AS destination_chain_id,
        destination_chain,
        token_address,
        token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__axelar_contractcallwithtoken') }}

{% if is_incremental() and 'axelar' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__celer_cbridge_send') }}

{% if is_incremental() and 'celer_cbridge' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
ccip AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _log_id AS _id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__ccip_send_requested') }}

{% if is_incremental() and 'ccip' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
dln_debridge AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__dln_debridge_createdorder') }}

{% if is_incremental() and 'dln_debridge' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
eywa AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__eywa_requestsent') }}

{% if is_incremental() and 'eywa' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__hop_transfersenttol2') }}

{% if is_incremental() and 'hop' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
layerzero_v2 AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _log_id AS _id,
        inserted_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__layerzero_v2') }}

{% if is_incremental() and 'layerzero_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
meson AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__meson_transfers') }}

{% if is_incremental() and 'meson' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__multichain_v7_loganyswapout') }}

{% if is_incremental() and 'multichain' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
stargate AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__stargate_swap') }}

{% if is_incremental() and 'stargate' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
stargate_v2 AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _log_id AS _id,
        inserted_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__stargate_v2') }}

{% if is_incremental() and 'stargate_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__symbiosis_synthesizerequest') }}

{% if is_incremental() and 'symbiosis' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
synapse_tb AS (
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
        'v1-tb' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__synapse_token_bridge') }}

{% if is_incremental() and 'synapse_tb' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
synapse_tbs AS (
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
        'v1-tbs' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__synapse_tokenbridgeandswap') }}

{% if is_incremental() and 'synapse_tbs' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
wormhole AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__wormhole_transfers') }}

{% if is_incremental() and 'wormhole' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
cctp AS (
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
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _log_id AS _id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__cctp_depositforburn') }}

{% if is_incremental() and 'cctp' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),
cctp_v2 AS (
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
        'v2' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _log_id AS _id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_bridge__cctp_v2_depositforburn') }}

{% if is_incremental() and 'cctp_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
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
        across_v3
    UNION ALL
    SELECT
        *
    FROM
        allbridge
    UNION ALL
    SELECT
        *
    FROM
        allbridge_v2
    UNION ALL
    SELECT
        *
    FROM
        axie_infinity_v2
    UNION ALL
    SELECT
        *
    FROM
        axelar
    UNION ALL
    SELECT
        *
    FROM
        celer_cbridge
    UNION ALL
    SELECT
        *
    FROM
        ccip
    UNION ALL
    SELECT
        *
    FROM
        dln_debridge
    UNION ALL
    SELECT
        *
    FROM
        eywa
    UNION ALL
    SELECT
        *
    FROM
        hop
    UNION ALL
    SELECT
        *
    FROM
        layerzero_v2
    UNION ALL
    SELECT
        *
    FROM
        meson
    UNION ALL
    SELECT
        *
    FROM
        multichain
    UNION ALL
    SELECT
        *
    FROM
        stargate
    UNION ALL
    SELECT
        *
    FROM
        stargate_v2
    UNION ALL
    SELECT
        *
    FROM
        symbiosis
    UNION ALL
    SELECT
        *
    FROM
        synapse_tb
    UNION ALL
    SELECT
        *
    FROM
        synapse_tbs
    UNION ALL
    SELECT
        *
    FROM
        wormhole
    UNION ALL
    SELECT
        *
    FROM
        cctp
    UNION ALL
    SELECT
        *
    FROM
        cctp_v2
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
        destination_chain_receiver,
        NULL AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
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

{% if is_incremental() and 'native_bridges' not in var('HEAL_MODELS') %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
        {{ this }}
)
{% endif %}

{% if is_incremental() and 'native_bridges' in var('HEAL_MODELS') %}
AND tx_hash NOT IN (
    SELECT
        DISTINCT tx_hash
    FROM
        {{ this }}
    WHERE
        version <> 'v1-native'
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
complete_bridge_activity AS (
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
        destination_chain_receiver,
        CASE
            WHEN CONCAT(
                platform,
                '-',
                version
            ) IN (
                'stargate-v1',
                'wormhole-v1',
                'meson-v1',
                'allbridge-v2',
                'circle-cctp-v1',
                'circle-cctp-v2-v2',
                'chainlink-ccip-v1',
                'stargate-v2',
                'layerzero-v2'
            ) THEN destination_chain_id :: STRING
            WHEN d.chain_id IS NULL THEN destination_chain_id :: STRING
            ELSE d.chain_id :: STRING
        END AS destination_chain_id,
        CASE
            WHEN CONCAT(
                platform,
                '-',
                version
            ) IN (
                'stargate-v1',
                'wormhole-v1',
                'meson-v1',
                'allbridge-v2',
                'circle-cctp-v1',
                'circle-cctp-v2-v2',
                'chainlink-ccip-v1',
                'stargate-v2',
                'layerzero-v2'
            ) THEN LOWER(destination_chain)
            WHEN d.chain IS NULL THEN LOWER(destination_chain)
            ELSE LOWER(
                d.chain
            )
        END AS destination_chain,
        b.token_address,
        CASE
            WHEN platform = 'axelar' THEN COALESCE(
                C.symbol,
                b.token_symbol
            )
            ELSE C.symbol
        END AS token_symbol,
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
        p.is_verified as token_is_verified,
        _id,
        b._inserted_timestamp
    FROM
        all_bridges b
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON b.token_address = C.address
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
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
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
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
        destination_chain_receiver,
        destination_chain_id,
        destination_chain,
        t0.token_address,
        C.symbol AS token_symbol,
        C.decimals AS token_decimals,
        amount_unadj,
        CASE
            WHEN C.decimals IS NOT NULL THEN (amount_unadj / pow(10, C.decimals))
            ELSE amount_unadj
        END AS amount_heal,
        CASE
            WHEN C.decimals IS NOT NULL THEN ROUND(
                amount_heal * p.price,
                2
            )
            ELSE NULL
        END AS amount_usd_heal,
        p.is_verified as token_is_verified,
        _id,
        t0._inserted_timestamp
    FROM
        {{ this }}
        t0
        LEFT JOIN {{ ref('core__dim_contracts') }} C
        ON t0.token_address = C.address
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON t0.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
    WHERE
        CONCAT(
            t0.block_number,
            '-',
            t0.platform,
            '-',
            t0.version
        ) IN (
            SELECT
                CONCAT(
                    t1.block_number,
                    '-',
                    t1.platform,
                    '-',
                    t1.version
                )
            FROM
                {{ this }}
                t1
            WHERE
                t1.token_decimals IS NULL
                AND t1._inserted_timestamp < (
                    SELECT
                        MAX(
                            _inserted_timestamp
                        ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        {{ ref('core__dim_contracts') }} C
                    WHERE
                        C.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND C.decimals IS NOT NULL
                        AND C.address = t1.token_address)
                    GROUP BY
                        1
                )
                OR CONCAT(
                    t0.block_number,
                    '-',
                    t0.platform,
                    '-',
                    t0.version
                ) IN (
                    SELECT
                        CONCAT(
                            t2.block_number,
                            '-',
                            t2.platform,
                            '-',
                            t2.version
                        )
                    FROM
                        {{ this }}
                        t2
                    WHERE
                        t2.amount_usd IS NULL
                        AND t2._inserted_timestamp < (
                            SELECT
                                MAX(
                                    _inserted_timestamp
                                ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
                            FROM
                                {{ this }}
                        )
                        AND EXISTS (
                            SELECT
                                1
                            FROM
                                {{ ref('silver__complete_token_prices') }}
                                p
                            WHERE
                                p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                                AND p.price IS NOT NULL
                                AND p.token_address = t2.token_address
                                AND p.hour = DATE_TRUNC(
                                    'hour',
                                    t2.block_timestamp
                                )
                        )
                    GROUP BY
                        1
                )
        ),
        newly_verified_tokens as (
          select token_address
          from {{ ref('price__ez_asset_metadata') }}
          where ifnull(is_verified_modified_timestamp, '1970-01-01' :: TIMESTAMP) > dateadd('day', -10, SYSDATE())
        ),
        heal_newly_verified_tokens as (
            SELECT
                t0.block_number,
                t0.block_timestamp,
                t0.origin_from_address,
                t0.origin_to_address,
                t0.origin_function_signature,
                t0.tx_hash,
                t0.event_index,
                t0.bridge_address,
                t0.event_name,
                t0.platform,
                t0.version,
                t0.sender,
                t0.receiver,
                t0.destination_chain_receiver,
                t0.destination_chain_id,
                t0.destination_chain,
                t0.token_address,
                t0.token_symbol,
                t0.token_decimals,
                t0.amount_unadj,
                t0.amount,
                CASE
                    WHEN t0.token_decimals IS NOT NULL THEN t0.amount * p.price
                    ELSE NULL
                END AS amount_usd_heal,
                p.is_verified AS token_is_verified,
                t0._id,
                t0._inserted_timestamp
            from {{ this }} t0
            join newly_verified_tokens nv
            on t0.token_address = nv.token_address
            left join {{ ref('price__ez_prices_hourly')}} p
            on t0.token_address = p.token_address
            and date_trunc('hour', t0.block_timestamp) = p.hour
        ),
    {% endif %}

    FINAL AS (
        SELECT
            *
        FROM
            complete_bridge_activity

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
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
    destination_chain_receiver,
    destination_chain_id,
    destination_chain,
    token_address,
    token_symbol,
    token_decimals,
    amount_unadj,
    amount_heal AS amount,
    amount_usd_heal AS amount_usd,
    token_is_verified,
    _id,
    _inserted_timestamp
FROM
    heal_model
UNION ALL
SELECT
    *
FROM
    heal_newly_verified_tokens
{% endif %}
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
    destination_chain_receiver,
    destination_chain_id,
    destination_chain,
    token_address,
    token_symbol,
    token_decimals,
    amount_unadj,
    amount,
    amount_usd,
    ifnull(token_is_verified, false) AS token_is_verified,
    _id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['_id']
    ) }} AS complete_bridge_activity_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    destination_chain <> 'ethereum' qualify (ROW_NUMBER() over (PARTITION BY _id
ORDER BY
    _inserted_timestamp DESC)) = 1
