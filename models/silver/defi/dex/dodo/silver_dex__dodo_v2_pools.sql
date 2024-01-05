{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "pool_address",
    tags = ['curated']
) }}

WITH pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS baseToken,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS quoteToken,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS creator,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS pool_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs') }}
    WHERE
        contract_address IN (
            '0x95e887adf9eaa22cc1c6e3cb7f07adc95b4b25a8',
            '0x5336ede8f971339f6c0e304c66ba16f1296a2fbe',
            '0x6b4fa0bc61eddc928e0df9c7f01e407bfcd3e5ef',
            '0xb5dc5e183c2acf02ab879a8569ab4edaf147d537',
            --dpp - factory,
            '0x6fddb76c93299d985f4d3fc7ac468f9a168577a4',
            --dsp - factory
            '0x79887f65f83bdf15bcc8736b5e5bcdb48fb8fe13',
            '0x72d220ce168c4f361dd4dee5d826a01ad8598f6c' --dvm - factory
        )
        AND topics [0] :: STRING IN (
            '0x8494fe594cd5087021d4b11758a2bbc7be28a430e94f2b268d668e5991ed3b8a',
            --NewDPP
            '0xbc1083a2c1c5ef31e13fb436953d22b47880cf7db279c2c5666b16083afd6b9d',
            --NewDSP
            '0xaf5c5f12a80fc937520df6fcaed66262a4cc775e0f3fceaf7a7cfe476d9a751d' --NewDVM
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address,
    baseToken AS base_token,
    quoteToken AS quote_token,
    creator,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
