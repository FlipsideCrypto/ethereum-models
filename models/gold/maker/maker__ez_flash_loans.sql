{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH mkr_txs AS (
    SELECT
        tx_hash
    FROM 
        {{ ref('silver__logs') }}
    WHERE 
        contract_address = '0x5ef30b9986345249bc32d8928b7ee64de9435e39'
        AND contract_name = 'DssCdpManager'
), 
get_loans AS (
    SELECT 
        block_number, 
        block_timestamp, 
        m.tx_hash, 
        tx_status, 
        COALESCE(
            event_inputs :_target :: STRING, 
            event_inputs :initiator :: STRING
        ) AS borrower, 
        COALESCE(
            event_inputs :_amount :: NUMBER, 
            event_inputs :amount :: NUMBER
        ) AS amount_loaned, 
        COALESCE(
            event_inputs :_reserve :: STRING, 
            event_inputs :asset :: STRING
        ) AS token_loaned, 
        l._inserted_timestamp, 
        _log_id 
    FROM mkr_txs m
        
    INNER JOIN {{ ref('silver__logs') }} l
    ON m.tx_hash = l.tx_hash 
    WHERE 
        event_name = 'FlashLoan'

    {% if is_incremental() %}
    AND
        l._inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) 
            FROM
                {{ this }}
        )
    {% endif %}
) 
SELECT
    m.block_number, 
    m.block_timestamp, 
    m.tx_hash, 
    m.tx_status, 
    COALESCE(
        borrower,
        event_inputs :to :: STRING
    ) AS borrower,  
    event_inputs :from :: STRING AS lender, 
    COALESCE(
        token_loaned,
        contract_address
    ) AS token_loaned,  
    c.symbol, 
    COALESCE(
        amount_loaned,
        event_inputs :value 
    ) AS amount_loaned, 
    c.decimals,  
    m._inserted_timestamp, 
    m._log_id  
FROM 
    get_loans m

INNER JOIN {{ ref('silver__logs') }} l
ON m.tx_hash = l.tx_hash

LEFT OUTER JOIN {{ ref('core__dim_contracts') }} c
ON m.token_loaned = c.address

WHERE 
    event_name = 'Transfer'

qualify(ROW_NUMBER() over(PARTITION BY m.tx_hash
ORDER BY
    event_index ASC)) = 1
