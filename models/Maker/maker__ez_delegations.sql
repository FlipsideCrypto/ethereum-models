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
)

SELECT 
    block_number, 
    block_timestamp, 
    v.tx_hash, 
    tx_status,
    contract_address, 
    CASE 
        WHEN event_name = 'Lock' THEN 
            'delegate'
        WHEN event_name = 'Free' THEN
            'undelegate'
    END AS tx_event, 
    CASE 
        WHEN event_name = 'Lock' THEN
            event_inputs :LockAmount / POW(10, 18) :: FLOAT
        WHEN event_name = 'Free' THEN
            event_inputs :wad / POW(10, 18) :: FLOAT
    END AS amount_delegated, 
    _inserted_timestamp, 
    _log_id
FROM vote_txs v

LEFT OUTER JOIN {{ ref('silver__logs') }} l 
ON v.tx_hash = l.tx_hash

WHERE 
    (event_name = 'Lock'
    OR event_name = 'Free')

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
