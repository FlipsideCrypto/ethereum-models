{{ config(
    materialized = "table",
    post_hook = "{{ grant_data_share_statement('DIM_DATES', 'TABLE') }}",
    tags = ['non_real_time']
) }}
{{ dbt_date.get_date_dimension(
    '2017-01-01',
    '2027-12-31'
) }}