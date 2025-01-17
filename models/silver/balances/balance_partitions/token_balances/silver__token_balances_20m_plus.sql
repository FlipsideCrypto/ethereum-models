{{ config(
    materialized = 'incremental',
    unique_key = ['block_number', 'address', 'contract_address'],
    cluster_by = ['block_number'],
    tags = ['curated']
) }}

SELECT 
    block_number,
    block_timestamp,
    address,
    contract_address,
    balance,
    _inserted_timestamp
FROM {{ ref('silver__token_balances') }}
WHERE block_number >= 20000000
{% if is_incremental() %}
  AND _inserted_timestamp >= (SELECT MAX(_inserted_timestamp) FROM {{ this }})
{% endif %}