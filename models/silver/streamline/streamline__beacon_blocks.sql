{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

{% set height = run_query('SELECT streamline.udf_get_beacon_chainhead()') %}

{% if execute %}
{% set block_height = height.columns[0].values()[0] %}
{% else %}
{% set block_height = 0 %}
{% endif %}

SELECT
    *
FROM
    TABLE(streamline.udtf_get_blocks_table({{block_height}}))
WHERE
    height >= 4700013 -- Start slot for ETH 2.0 Beacon chain data
