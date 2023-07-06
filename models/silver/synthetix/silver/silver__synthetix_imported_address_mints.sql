{{ config(
    materialized = 'incremental',
    unique_key = 'wallet_tx_hash',
    cluster_by = ['block_timestamp::DATE']
) }}


WITH imported_address_traces AS ( 
    SELECT 
        block_timestamp,
        tx_hash,
        _inserted_timestamp 
    FROM 
        {{ ref('silver__traces') }}
    WHERE 
        input ILIKE '0x8f849518%'
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(traces_timestamp)
        FROM
            {{ this }}
    )
    {% endif %} 
),
ranked_traces AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tx_hash
            ORDER BY block_timestamp DESC
        ) AS row_num
    FROM
        imported_address_traces
)
SELECT 
    l.block_number, 
    l.block_timestamp, 
    l.tx_hash, 
    l.event_index, 
    decoded_flat :account AS wallet_address,
    concat_ws('',l.tx_hash,l.decoded_flat :account) as wallet_tx_hash, 
    CAST(decoded_flat :amount AS DECIMAL) / pow(10,18) AS minted_amount,
    l._inserted_timestamp as logs_timestamp,
    t._inserted_timestamp as traces_timestamp
FROM 
    {{ ref('silver__decoded_logs') }} l
LEFT JOIN
    ranked_traces t
ON
    l.tx_hash = t.tx_hash
    AND t.row_num = 1
WHERE 
    l.tx_hash IN ( 
        SELECT 
            tx_hash 
        FROM 
            imported_address_traces 
    ) 
    AND tx_status = 'SUCCESS' 
    AND event_name = 'Mint'
    {% if is_incremental() %}
    AND l._inserted_timestamp >= (
        SELECT
            MAX(logs_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}  
