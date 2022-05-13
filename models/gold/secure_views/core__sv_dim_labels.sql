{{ config(
    materialized = 'view',
    secure = true,
    pre_hook = "call silver.sp_create_cross_db_share_clones()",
    post_hook = "{{ grant_data_share_statement('SV_DIM_LABELS', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ source('ethereum_share','labels')}}
WHERE
    blockchain = 'ethereum'
    AND address LIKE '0x%'
