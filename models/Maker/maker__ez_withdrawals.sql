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

get_withdrawals AS ( 
    SELECT 
        block_number, 
        block_timestamp, 
        tx_hash,
        tx_status, 
        origin_from_address AS withdrawer, 
        origin_to_address AS vault, 
        contract_address AS token_withdrawn,
        event_inputs :wad / POW(10, 18) AS amount_withdrawn, 
        event_name, 
        _inserted_timestamp, 
        _log_id
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND event_name = 'Withdrawal'

{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) 
        FROM
            max_date
    )
{% endif %}
)

SELECT 
    d.block_number, 
    d.block_timestamp, 
    d.tx_hash, 
    d.tx_status, 
    withdrawer, 
    vault, 
    token_withdrawn, 
    amount_withdrawn, 
    e.contract_address AS token_transferred, 
    e.event_inputs :value / POW(10, 18) AS amount_transferred, 
    e._inserted_timestamp, 
    d._log_id
FROM get_withdrawals d

LEFT OUTER JOIN {{ ref('silver__logs') }} e
ON d.tx_hash = e.tx_hash 
AND d.withdrawer = e.event_inputs :from :: STRING 

WHERE e.contract_name = 'Dai' 
AND e.event_name = 'Transfer'

{% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}