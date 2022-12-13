{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    full_refresh = false
) }}

SELECT
    *
FROM
    {{ source(
        'bronze_api_dev',
        'snapshot_votes_historical'
    ) }}