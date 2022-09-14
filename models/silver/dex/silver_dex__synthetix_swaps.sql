{{ config(
    materialized = 'incremental',
    unique_key = "_log_id",
    cluster_by = ['block_timestamp::DATE']
) }}
-- build base table
WITH synthetix_swaps_base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        'synthetix' AS "POOL_NAME",
        'Swap' AS "EVENT_NAME",
        origin_from_address AS "SENDER",
        SUBSTR(
            event_inputs :toAddress,
            1,
            42
        ) AS "TX_TO",
        -- substr to remove " at the end and start of tx_to
        -- 18 decimals is hardcoded on the synth contracts https://docs.synthetix.io/contracts/source/contracts/Synth/
        event_inputs :fromAmount / 1e18 AS "AMOUNT_IN",
        event_inputs :toAmount / 1e18 AS "AMOUNT_OUT",
        event_index,
        'synthetix' AS "PLATFORM",
        -- remove "0x" from the start of the currency key -> decode value -> filter out null characters
        REGEXP_REPLACE(
            HEX_DECODE_STRING(SUBSTR(event_inputs :fromCurrencyKey, 3, 64)),
            '[^a-zA-Z0-9]+'
        ) AS "SYMBOL_IN",
        REGEXP_REPLACE(
            HEX_DECODE_STRING(SUBSTR(event_inputs :toCurrencyKey, 3, 64)),
            '[^a-zA-Z0-9]+'
        ) AS "SYMBOL_OUT",
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        event_inputs,
        contract_address,
        _inserted_timestamp,
        -- used for incremental materialization,
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS swap_hour -- helper column
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f',
            '0xc011a72400e58ecd99ee497cf89e3775d4bd732f'
        ) -- synthetics proxy
        AND event_name = 'SynthExchange'
        {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) :: DATE - 2
            FROM
                {{ this }}
        )
        {% endif %}

),
-- get token addresses
synthetix_swaps_with_token_addresses AS (
    SELECT
        *
    FROM
        synthetix_swaps_base
        LEFT JOIN (
            SELECT
                synth_symbol AS synth_symbol_in,
                synth_proxy_address AS token_in
            FROM
                {{ ref('silver__synthetix_synths') }}
        ) synths_in
        ON synths_in.synth_symbol_in = synthetix_swaps_base.symbol_in
        LEFT JOIN (
            SELECT
                synth_symbol AS synth_symbol_out,
                synth_proxy_address AS token_out
            FROM
                {{ ref('silver__synthetix_synths') }}
        ) synths_out
        ON synths_out.synth_symbol_out = synthetix_swaps_base.symbol_out
)
-- add token prices
    SELECT
        synthetix_swaps_with_token_addresses.block_number,
        synthetix_swaps_with_token_addresses.block_timestamp,
        synthetix_swaps_with_token_addresses.tx_hash,
        synthetix_swaps_with_token_addresses.origin_function_signature,
        synthetix_swaps_with_token_addresses.origin_from_address,
        synthetix_swaps_with_token_addresses.origin_to_address,
        synthetix_swaps_with_token_addresses.contract_address AS contract_address,
        synthetix_swaps_with_token_addresses.pool_name,
        synthetix_swaps_with_token_addresses.event_name,
        synthetix_swaps_with_token_addresses.amount_in,
        synthetix_swaps_with_token_addresses.amount_in * prices_in.price_in AS amount_in_usd,
        synthetix_swaps_with_token_addresses.amount_out,
        synthetix_swaps_with_token_addresses.amount_out * prices_out.price_out AS amount_out_usd,
        synthetix_swaps_with_token_addresses.sender,
        synthetix_swaps_with_token_addresses.tx_to,
        synthetix_swaps_with_token_addresses.event_index,
        synthetix_swaps_with_token_addresses.platform,
        synthetix_swaps_with_token_addresses.token_in,
        synthetix_swaps_with_token_addresses.token_out,
        synthetix_swaps_with_token_addresses.symbol_in,
        synthetix_swaps_with_token_addresses.symbol_out,
        synthetix_swaps_with_token_addresses._log_id,
        synthetix_swaps_with_token_addresses._inserted_timestamp
    FROM
        synthetix_swaps_with_token_addresses
        LEFT JOIN (
            SELECT
                HOUR AS hour_in,
                price AS price_in,
                token_address AS token_address_in
            FROM
                {{ ref('core__fact_hourly_token_prices') }}
        ) prices_in
        ON prices_in.token_address_in = synthetix_swaps_with_token_addresses.token_in
        AND prices_in.hour_in = synthetix_swaps_with_token_addresses.swap_hour
        LEFT JOIN (
            SELECT
                HOUR AS hour_out,
                price AS price_out,
                token_address AS token_address_out
            FROM
                {{ ref('core__fact_hourly_token_prices') }}
        ) prices_out
        ON prices_out.token_address_out = synthetix_swaps_with_token_addresses.token_out
        AND prices_out.hour_out = synthetix_swaps_with_token_addresses.swap_hour
