{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}


{% if execute %}
{% set height = run_query('SELECT streamline.udf_get_chainhead()') %}
{% set block_height = height.columns[0].values()[0] %}
{% else %}
{% set block_height = 0 %}
{% endif %}

SELECT
    height as block_number,
    REPLACE(CONCAT_WS('', '0x', TO_CHAR(block_number, 'XXXXXXXX')), ' ', '') AS block_number_hex
FROM
    TABLE(streamline.udtf_get_base_table({{block_height}}))
WHERE
    height >= 15537394 -- Start block for ETH 2.0 Mainnet chain data