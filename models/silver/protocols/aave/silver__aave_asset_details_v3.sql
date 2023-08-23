{{ config(
    materialized = 'view',
    tags = ['non_realtime'],
) }}

SELECT
    C.symbol, 
    C.name, 
    C.decimals, 
    decoded_flat :reserve :: STRING AS reserve_asset
FROM
    {{ ref('silver__decoded_logs') }}
    l
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON decoded_flat :reserve :: STRING = C.address
WHERE
    contract_address = '0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2'
    AND event_name IN ('Supply', 'Borrow', 'Repay', 'Withdraw')
GROUP BY
    ALL
