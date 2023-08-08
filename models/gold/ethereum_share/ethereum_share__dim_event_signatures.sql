{{ config(
    materialized = 'incremental',
    unique_key = "hex_signature",
    cluster_by = ['hex_signature'],
    tags = ['share']
) }}

SELECT
    text_signature,
    hex_signature,
    id
FROM
    {{ ref('core__dim_event_signatures') }}