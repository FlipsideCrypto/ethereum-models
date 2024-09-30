{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'modified_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, label_type, label_subtype, address_name, project_name), SUBSTRING(address, label_type, label_subtype, address_name, project_name); DELETE FROM {{ this }} WHERE _is_deleted = TRUE;",
    tags = ['non_realtime']
) }}
{{ fsc_evm.silver_labels() }}