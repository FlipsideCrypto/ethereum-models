{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id'
) }}

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash, 
    tx_status,  
    event_index, 
    event_inputs :voter :: STRING AS voter, 
    contract_address AS polling_contract, 
    event_inputs :optionId :: INTEGER AS vote_option, 
    event_inputs :pollId :: INTEGER AS proposal_id
FROM 
    {{ ref('silver__logs') }}
WHERE 
    contract_name = 'PollingEmitter'
    AND event_name = 'Voted'

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