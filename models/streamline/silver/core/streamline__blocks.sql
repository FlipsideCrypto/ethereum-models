{{ config (
    materialized = "view",
    tags = ['streamline_core_complete']
) }}

{% if execute %}
    {% set height = run_query('SELECT streamline.udf_get_chainhead()') %}
    {% set block_height = height.columns [0].values() [0] %}
{% else %}
    {% set block_height = 0 %}
{% endif %}

SELECT
    _id AS block_number,
    REPLACE(
        concat_ws('', '0x', to_char(block_number, 'XXXXXXXX')),
        ' ',
        ''
    ) AS block_number_hex
FROM
    {{ ref("silver__number_sequence") }}
WHERE
    _id <= {{ block_height }}
ORDER BY
    _id ASC
