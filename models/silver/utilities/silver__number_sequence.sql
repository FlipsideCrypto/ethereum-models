{{ config(
    materialized = 'table',
    cluster_by = 'round(_id,-3)',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}
{{ fsc_evm.number_sequence(
    max_num = 50000000
) }}
