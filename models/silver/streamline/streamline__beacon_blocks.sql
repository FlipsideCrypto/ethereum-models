{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

{% if execute %}
{% set height = run_query('SELECT streamline.udf_get_beacon_chainhead()') %}
{% set block_height = height.columns[0].values()[0] %}
{% else %}
{% set slot_height = 0 %}
{% endif %}

SELECT
    height as slot_number
FROM
    TABLE(streamline.udtf_get_base_table({{slot_height}}))
WHERE
    height >= 4700013 -- Start slot for ETH 2.0 Beacon chain data
