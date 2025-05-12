{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','decoded_logs','phase_2']
) }}

SELECT
    *
FROM
    {{ ref('bronze__decoded_logs_fr_v2') }}
UNION ALL
SELECT
    *
FROM
    {{ ref('bronze__decoded_logs_fr_v1') }}