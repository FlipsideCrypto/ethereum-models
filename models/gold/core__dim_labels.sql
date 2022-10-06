{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name AS label
FROM
    {{ source(
        'crosschain',
        'address_labels'
    ) }} 
WHERE
    blockchain = 'ethereum'
    AND address LIKE '0x%'
