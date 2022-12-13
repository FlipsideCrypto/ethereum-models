{{ config(
    materialized = 'incremental',
    unique_key = 'proposal_id',
    full_refresh = false
) }}

SELECT
    *
FROM
    {{ source(
        'bronze_api_dev',
        'snapshot_proposals_historical'
    ) }}