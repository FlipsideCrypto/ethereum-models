{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH get_maker_txs AS (
    SELECT 
        tx_hash
    FROM {{ ref('silver__logs') }} 

    WHERE 
        contract_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'
    
    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) 
            FROM
                {{ this }}
        )
    {% endif %}
), 
other_events AS (
    SELECT 
        tx_hash 
    FROM get_maker_txs m

    INNER JOIN {{ ref('silver__logs') }} l
    ON l.tx_hash = m.tx_hash

    WHERE 
        (event_name = 'Withdrawal'
        OR event_name = 'Deposit')
    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) 
            FROM
                {{ this }}
        )
    {% endif %}
)

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash,
    tx_status, 
    origin_from_address AS payer, 
    origin_to_address AS vault, 
    contract_address AS token_paid,
    symbol, 
    event_inputs :value AS amount_paid, 
    decimals,  
    _inserted_timestamp, 
    _log_id
FROM 
    {{ ref('silver__logs') }} l 

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
ON d.token_paid = c.address

WHERE 
    tx_hash NOT IN (
        SELECT 
            tx_hash
        FROM other_events
    )
    AND contract_name = 'Dai' 
    AND event_name = 'Transfer'
    AND origin_from_address = event_inputs :from :: STRING

{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) 
        FROM
            {{ this }}
    )
{% endif %}


