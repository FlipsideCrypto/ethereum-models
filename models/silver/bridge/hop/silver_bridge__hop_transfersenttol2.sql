
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
        'hop' AS name,
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        decoded_flat:"amount"::INTEGER AS amount, decoded_flat:"amountOutMin"::INTEGER AS amountOutMin, decoded_flat:"chainId"::INTEGER AS chainId, decoded_flat:"deadline"::INTEGER AS deadline, decoded_flat:"recipient"::STRING AS recipient, decoded_flat:"relayer"::STRING AS relayer, decoded_flat:"relayerFee"::INTEGER AS relayerFee,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__decoded_logs') }}
    WHERE 
        topics[0] :: STRING = '0x0a0607688c86ec1775abcdbab7b33a3a35a6c9cde677c9be880150c231cc6b0b'
        

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{ this }}
    )
    {% endif %}
    