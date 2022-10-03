{{ config(
    materialized = 'view',
    tags = ['streamline_view'],
    post_hook = if_data_call_function(
      func = "{{this.schema}}.udf_streamline_produce()",
      target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT * FROM {{ ref('evm_queues') }}
UNION ALL
SELECT * FROM {{ ref('derived_queues') }}