{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH cat_bite AS (
    SELECT
        block_number, 
        block_timestamp, 
        tx_hash, 
        tx_status, 
        origin_from_address, 
        origin_to_address, 
        contract_address, 
        event_inputs :ilk :: STRING AS collateral, 
        event_inputs :ink / POW(10, 18) :: FLOAT AS collateral_balance, 
        event_inputs :art :: FLOAT AS normalized_stablecoin_debt, 
        event_inputs :urn :: STRING AS vault, 
        _inserted_timestamp, 
        _log_id
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        contract_name = 'Cat'
        AND event_name = 'Bite'

    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                )
            FROM
                {{ this }}
        )
    {% endif %}
),

transactions AS (
    SELECT
        tx_hash
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        contract_name = 'Spotter'
        AND event_name = 'Poke'
    
    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                )
            FROM
                {{ this }}
        )
    {% endif %}
)

SELECT 
    block_number, 
    block_timestamp, 
    c.tx_hash, 
    tx_status, 
    origin_from_address, 
    origin_to_address, 
    contract_address, 
    collateral, 
    collateral_balance, 
    normalized_stablecoin_debt, 
    vault, 
    event_inputs :user :: STRING AS liquidated_wallet, 
    event_inputs :gal :: STRING AS liquidator, 
    event_inputs :id :: INTEGER AS auction_id, 
    _inserted_timestamp, 
    _log_id
FROM cat_bite c

INNER JOIN {{ ref('silver__logs') }} l
ON c.tx_hash = l.tx_hash

INNER JOIN transactions t
ON c.tx_hash = t.tx_hash

WHERE 
    l.event_name = 'Kick'
{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}