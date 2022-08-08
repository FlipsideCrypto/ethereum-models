{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH get_withdrawals AS ( 
    SELECT 
        block_number, 
        block_timestamp, 
        tx_hash,
        tx_status, 
        origin_from_address AS withdrawer, 
        origin_to_address AS vault, 
        _inserted_timestamp, 
        _log_id
    FROM 
        {{ ref('silver__logs') }}
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
transfer_amt AS (
    SELECT
        w.block_number, 
        w.block_timestamp, 
        w.tx_hash, 
        w.tx_status, 
        withdrawer, 
        vault, 
        contract_address AS token_withdrawn,
        event_inputs :wad AS amount_withdrawn, 
        e._inserted_timestamp, 
        w._log_id
    FROM get_withdrawals w

    INNER JOIN {{ ref('silver__logs') }} e
    ON w.tx_hash = e.tx_hash

    WHERE e.event_name = 'Withdrawal'

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
)

SELECT 
    d.block_number, 
    d.block_timestamp, 
    d.tx_hash, 
    d.tx_status, 
    withdrawer, 
    vault, 
    token_withdrawn, 
    c.symbol
    amount_withdrawn, 
    c.decimals, 
    _inserted_timestamp, 
    d._log_id
FROM transfer_amt d

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
ON d.token_withdrawn = c.address