{{ config(
    materialized = 'incremental',
    tags = ['curated']
) }}

WITH DECODE AS (

    SELECT
        block_number,
        tx_hash,
        decoded_flat:name::string as name,
        decoded_flat:symbol::string as symbol,
        contract_address,
        decoded_flat:asset::string as underlying_asset,
        decoded_flat:asset::string as caller,
        decoded_flat:metaMorpho::string as token_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__decoded_logs') }}
        l
    WHERE
        TOPICS[0]::STRING = '0xed8c95d05909b0f217f3e68171ef917df4b278d5addfe4dda888e90279be7d1d'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND token_address NOT IN (
    SELECT
        token_address
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    tx_hash,
    token_address,
    caller,
    a.name as token_name,
    a.symbol as token_symbol,
    c2.decimals as token_decimals,
    contract_address,
    underlying_asset,
    c1.name as underlying_name,
    c1.symbol as underlying_symbol,
    c1.decimals as underlying_decimals,
    A._inserted_timestamp,
    A._log_id
FROM
    decode A
    LEFT JOIN {{ ref('silver__contracts') }} c1
    ON c1.address = A.underlying_asset 
    LEFT JOIN {{ ref('silver__contracts') }} c2
    ON c2.address = A.token_address qualify(ROW_NUMBER() over(PARTITION BY token_address
ORDER BY
    a._inserted_timestamp DESC)) = 1
