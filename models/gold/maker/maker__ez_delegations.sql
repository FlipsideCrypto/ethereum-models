{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH vote_txs AS (
    SELECT 
        tx_hash
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        contract_address = '0x0a3f6849f78076aefadf113f5bed87720274ddc0' -- MakerDAO general governance contract
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

delegations AS (
    SELECT 
        v.tx_hash, 
        data :to :: STRING AS origin_from_address, 
        data :from :: STRING AS delegate, 
        t._inserted_timestamp
    FROM vote_txs v

    LEFT OUTER JOIN {{ ref('silver__traces') }} t
    ON v.tx_hash = t.tx_hash

    WHERE 
        type = 'CALL'
        AND identifier = 'CALL_ORIGIN'

    {% if is_incremental() %}
    AND
        t._inserted_timestamp >= (
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
    l.block_number, 
    l.block_timestamp, 
    v.tx_hash, 
    tx_status,
    t.origin_from_address, 
    contract_address, 
    CASE 
        WHEN event_name = 'Lock' THEN 
            'delegate'
        WHEN event_name = 'Free' THEN
            'undelegate'
    END AS tx_event,
    delegate,  
    CASE 
        WHEN event_name = 'Lock' THEN
            event_inputs :LockAmount :: FLOAT
        WHEN event_name = 'Free' THEN
            event_inputs :wad :: FLOAT
    END AS amount_delegated, 
    18 AS decimals,  
    l._inserted_timestamp, 
    _log_id
FROM vote_txs v

LEFT OUTER JOIN {{ ref('silver__logs') }} l 
ON v.tx_hash = l.tx_hash

LEFT OUTER JOIN delegations t
ON v.tx_hash = t.tx_hash

WHERE 
    (event_name = 'Lock'
    OR event_name = 'Free')

{% if is_incremental() %}
AND
    l._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}
