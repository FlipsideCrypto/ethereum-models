
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
        'synapse' AS name,
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        decoded_flat:"amount"::INTEGER AS amount, decoded_flat:"chainId"::INTEGER AS chainId, decoded_flat:"deadline"::INTEGER AS deadline, decoded_flat:"minDy"::INTEGER AS minDy, decoded_flat:"to"::STRING AS to_address, decoded_flat:"token"::STRING AS token, decoded_flat:"tokenIndexFrom"::INTEGER AS tokenIndexFrom, decoded_flat:"tokenIndexTo"::INTEGER AS tokenIndexTo,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__decoded_logs') }}
    WHERE 
        topics[0] :: STRING = '0x79c15604b92ef54d3f61f0c40caab8857927ca3d5092367163b4562c1699eb5f'
        AND contract_address = '0x2796317b0ff8538f253012862c06787adfb8ceb6'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{ this }}
    )
    {% endif %}
    