{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH
{% if is_incremental() %}
max_date AS (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
), 
{% endif %}

other_events AS (
    SELECT 
        tx_hash, 
        event_name 
    FROM {{ ref('silver__logs') }}
    WHERE 
        origin_to_address = '0x978410249203f7b5e6ef873f61229be2eae38c24' 
        AND (event_name = 'Withdrawal'
        OR event_name = 'Deposit')
)

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash,
    tx_status, 
    origin_from_address AS payer, 
    origin_to_address AS vault_contract, 
    -- vault number
    contract_address AS token_paid,
    event_inputs :value / POW(10, 18) AS amount_paid, 
    event_name, 
    _inserted_timestamp, 
    _log_id
FROM 
    {{ ref('silver__logs') }}
WHERE 
    origin_to_address = '0x978410249203f7b5e6ef873f61229be2eae38c24' --- change this when vault dimension table is available 
    AND tx_hash NOT IN (
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
            max_date
    )
{% endif %}


