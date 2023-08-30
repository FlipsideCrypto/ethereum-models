
    {{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    tags = ['non_realtime']
    ) }}
    
    SELECT 
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'ens' AS name,
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        decoded_flat:"proposalId"::STRING AS proposalId,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__decoded_logs') }}
    WHERE 
        topics[0] :: STRING = '0x712ae1383f79ac853f8d882153778e0260ef8f03b504e2866e0593e04d2b291f'
        AND contract_address = '0x323a76393544d5ecca80cd6ef2a560c6a395b7e3'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{ this }}
    )
    {% endif %}
    