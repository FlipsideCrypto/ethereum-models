{{ config(
    materialized = 'view',
    secure = true,
    post_hook = "{{ grant_data_share_statement('SV_DIM_LABELS', 'VIEW') }}"
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type AS l1_label,
    label_subtype AS l2_label,
    address_name,
    project_name,
    NULL AS delete_flag
FROM
    {{ ref('silver__labels') }}
