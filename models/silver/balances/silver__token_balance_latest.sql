{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'contract_address'],
    cluster_by = ['address', 'contract_address'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['curated']
) }}

WITH latest_states AS (
    SELECT 
        address,
        contract_address,
        balance,
        block_number,
        block_timestamp,
        _inserted_timestamp
    FROM {{ ref('silver__token_balances') }}
    {% if is_incremental() %}
    WHERE _inserted_timestamp >= (
        SELECT MAX(_inserted_timestamp)
        FROM {{ this }}
    )
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY address, contract_address 
        ORDER BY block_number DESC
    ) = 1
)
SELECT 
    address,
    contract_address,
    balance,
    block_number,
    block_timestamp,
    SYSDATE() as last_updated,
    _inserted_timestamp
FROM 
    latest_states