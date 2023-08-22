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
hop AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        tx_hash,
        event_index,
        contract_address,
        event_name,
        platform,
        sender,
        receiver,
        destination_chain_id,
        token_address,
        amount,
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
--token_symbol, --join with contracts for symbol
--amount_adj --join with contracts for decimals
--amount_usd --join with prices for price
--destination_chain --name of chain, join with defillama chains table
--convert chain symbol to id for allbridge