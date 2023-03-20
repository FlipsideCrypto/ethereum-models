{{ config (
    materialized = 'view'
) }}

{% set model = this.identifier.split("_") [-1] %}
{{ streamline_blocks_view(
    model
) }}
