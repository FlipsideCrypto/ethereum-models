{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = 'vault', 
  cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE']
) }}

SELECT
    block_number, 
    block_timestamp, 
    tx_hash, 
    tx_status, 
    origin_from_address AS creator, 
    COALESCE(
        origin_to_address, 
        event_inputs :own :: STRING
     ) AS vault,
    event_inputs :cdp :: NUMBER AS vault_number, 
    _inserted_timestamp 
FROM {{ ref('silver__logs') }}
WHERE 
    contract_name = 'DssCdpManager'
    AND event_name = 'NewCdp'

    {% if is_incremental() %}
    AND
        _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp) 
            FROM
                {{ this }}
        )
    {% endif %}

qualify(ROW_NUMBER() over(PARTITION BY vault
ORDER BY
    block_timestamp ASC)) = 1    
