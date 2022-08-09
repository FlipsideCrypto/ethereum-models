{{ config(
    materialized = 'view'
) }}

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash, 
    tx_status, 
    creator, 
    vault, 
    vault_number
FROM 
    {{ ref('silver_maker__vault_creation') }}