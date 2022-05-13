{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_DIM_EVENT_SIGNATURES', 'VIEW') }}"
) }}

SELECT
    *
FROM
    {{ ref('core__dim_event_signatures') }}
