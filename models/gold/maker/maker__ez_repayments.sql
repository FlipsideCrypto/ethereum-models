{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id', 
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE']
) }}

WITH get_repayments AS (
    SELECT   
        tx_hash    
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        contract_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'
        AND tx_hash NOT IN (
            SELECT 
                tx_hash
            FROM 
                {{ ref('silver__logs') }}
            WHERE 
                event_name = 'Deposit'
                OR event_name = 'Withdrawal'
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
    r.tx_hash,
    tx_status, 
    event_index, 
    origin_from_address AS payer, 
    origin_to_address AS vault, 
    contract_address AS token_paid,
    symbol, 
    event_inputs :value :: NUMBER AS amount_paid, 
    COALESCE(
        decimals, 
        18
    ) AS decimals, 
    l._inserted_timestamp,
    l._log_id
FROM 
    get_repayments r
    
INNER JOIN {{ ref('silver__logs') }} l 
ON r.tx_hash = l.tx_hash

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
ON contract_address = c.address

WHERE 
    contract_name = 'Dai' 
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


