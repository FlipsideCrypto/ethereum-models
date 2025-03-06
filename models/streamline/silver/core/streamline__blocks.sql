{%- if flags.WHICH == 'compile' and execute -%}

    {% set config_log = '\n' %}
    {% set config_log = config_log ~ '\n=== DBT Model Config ===\n'%}
    {% set config_log = config_log ~ '\n{{ config (\n' %}
    {% set config_log = config_log ~ '    materialized = "' ~ config.get('materialized') ~ '",\n' %}
    {% set config_log = config_log ~ '    tags = ' ~ config.get('tags') | tojson ~ '\n' %}
    {% set config_log = config_log ~ ') }}\n' %}
    {{ log(config_log, info=True) }}
    {{ log("", info=True) }}
{%- endif -%}

{{ config (
    materialized = "view",
    tags = ['streamline_core_complete']
) }}

SELECT
    _id,
    (
        ({{ var('GLOBAL_BLOCKS_PER_HOUR',0) }} / 60) * {{ var('GLOBAL_CHAINHEAD_DELAY',3) }}
    ) :: INT AS block_number_delay, --minute-based block delay
    (_id - block_number_delay) :: INT AS block_number,
    utils.udf_int_to_hex(block_number) AS block_number_hex
FROM
    {{ ref('silver__number_sequence') }}
WHERE
    _id <= (
        SELECT
            COALESCE(
                block_number,
                0
            )
        FROM
            {{ ref("streamline__get_chainhead") }}
    )