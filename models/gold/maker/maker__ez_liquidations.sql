{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id'
) }}

WITH cat_bite AS (
    SELECT
        block_number, 
        block_timestamp, 
        tx_hash, 
        tx_status, 
        event_inputs :ilk :: STRING AS collateral, 
        c.symbol, 
        event_inputs :ink :: NUMBER AS collateral_balance, 
        c.decimals, 
        event_inputs :art :: NUMBER AS normalized_stablecoin_debt, 
        event_inputs :urn :: STRING AS vault
    FROM 
        {{ ref('silver__logs') }} 

    LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
    ON event_inputs :ilk :: STRING = c.address

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

    qualify(ROW_NUMBER() over(PARTITION BY tx_hash
ORDER BY
    event_index ASC)) = 1
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
    c.block_number, 
    c.block_timestamp, 
    c.tx_hash, 
    c.tx_status, 
    collateral,
    symbol,  
    collateral_balance, 
    COALESCE(
        decimals,
        18
    ) AS decimals,  
    normalized_stablecoin_debt, 
    vault, 
    COALESCE(
        event_inputs :user :: STRING, 
        event_inputs :usr :: STRING 
     ) AS liquidated_wallet, 
    event_inputs :gal :: STRING AS liquidator, 
    event_inputs :id :: INTEGER AS auction_id
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

qualify(ROW_NUMBER() over(PARTITION BY l.tx_hash
ORDER BY
    event_index ASC)) = 1