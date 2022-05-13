{{ config(
    materialized = "table",
    post_hook = "{{ grant_data_share_statement('DIM_DATES', 'TABLE') }}"
) }}
{{ dbt_date.get_date_dimension(
    '2017-01-01',
    '2022-12-31'
) }}
