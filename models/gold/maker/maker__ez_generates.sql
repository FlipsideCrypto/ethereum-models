{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id'
) }}

WITH mkr_txs AS (
    SELECT 
        tx_hash
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        contract_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'
        AND contract_name = 'DssCdpManager'
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

get_borrows AS ( 
    SELECT 
        block_number, 
        block_timestamp, 
        m.tx_hash,
        tx_status, 
        origin_from_address AS borrower, 
        origin_to_address AS vault
    FROM mkr_txs m
        
    INNER JOIN {{ ref('silver__logs') }} l 
    ON m.tx_hash = l.tx_hash

    WHERE 
        event_name = 'Borrow'
     
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
        d.block_number, 
        d.block_timestamp, 
        d.tx_hash,
        d.tx_status, 
        event_index, 
        borrower, 
        vault, 
        contract_address AS token_generated, 
        COALESCE(
            event_inputs :value, 
            event_inputs :amount, 
            event_inputs :_amount
        ) AS amount_generated
    FROM get_borrows d

    INNER JOIN {{ ref('silver__logs') }} e
    ON d.tx_hash = e.tx_hash 

    WHERE 
        e.contract_name  = 'Dai'
        AND e.event_name = 'Deposit'

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
    event_index, 
    borrower, 
    vault, 
    token_generated, 
    c.symbol, 
    amount_generated,
    c.decimals
FROM transfer_amt d

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
ON d.token_generated = c.address