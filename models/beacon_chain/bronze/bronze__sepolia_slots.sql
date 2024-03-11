{{ config(
    materialized = 'incremental',
    unique_key = 'slot_number',
    full_refresh = false,
    tags = ['streamline_beacon_realtime']
) }}

SELECT
    live.udf_api(
        'GET',
        CONCAT(
            '{Service}',
            '/',
            '{Authentication}',
            'eth/v1/beacon/headers'
        ),{},{},
        'Vault/prod/ethereum/quicknode/sepolia'
    ) :data :data [0] :header :message :slot :: INT AS slot_number,
    SYSDATE() AS inserted_timestamp
