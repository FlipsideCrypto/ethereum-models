{{ config(
    materialized = 'incremental',
    unique_key = "bytes_signature",
    cluster_by = ['bytes_signature'],
    tags = ['share']
) }}

SELECT
    text_signature,
    bytes_signature,
    id
FROM
    {{ ref('core__dim_function_signatures') }}