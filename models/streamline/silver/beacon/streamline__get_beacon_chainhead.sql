{{ config (
    materialized = 'table',
    tags = ['streamline_beacon_complete']
) }}

SELECT
    {{ target.database }}.live.udf_api(
        'GET',
        '{service}/{Authentication}/eth/v1/beacon/headers',
        OBJECT_CONSTRUCT(
            'accept',
            'application/json'
        ),
        NULL,
        'vault/prod/ethereum/quicknode/mainnet'
    ) AS resp,
    resp :data :data [0] :header :message :slot :: INT AS slot_number
