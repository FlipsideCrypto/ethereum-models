{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id'
) }}

WITH get_withdrawals AS ( 
    SELECT 
        block_number, 
        block_timestamp, 
        tx_hash,
        tx_status, 
        origin_from_address AS withdrawer, 
        origin_to_address AS vault
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

    qualify(ROW_NUMBER() over(PARTITION BY tx_hash
ORDER BY
    event_index ASC)) = 1
), 
transfer_amt AS (
    SELECT
        w.block_number, 
        w.block_timestamp, 
        w.tx_hash, 
        w.tx_status, 
        event_index, 
        withdrawer, 
        vault, 
        contract_address AS token_withdrawn,
        COALESCE(
            event_inputs :wad :: NUMBER, 
            event_inputs :fee :: NUMBER
         ) AS amount_withdrawn
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
    d.event_index, 
    withdrawer, 
    vault, 
    token_withdrawn, 
    c.symbol, 
    amount_withdrawn, 
    COALESCE(
        c.decimals, 
        18
    ) AS decimals
FROM transfer_amt d

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
ON d.token_withdrawn = c.address