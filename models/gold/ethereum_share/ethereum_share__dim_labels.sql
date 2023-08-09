{{ config(
    materialized = 'incremental',
    unique_key = "address",
    cluster_by = ['address'],
    tags = ['share']
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    label
FROM
    {{ ref('core__dim_labels') }}