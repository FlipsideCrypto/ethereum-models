{{ config(
    materialized = "table",
    post_hook = "{{ grant_data_share_statement('DIM_DATES', 'TABLE') }}"
) }}
{{ dbt_date.get_date_dimension(
    '2021-12-01',
    '2021-12-31'
) }}