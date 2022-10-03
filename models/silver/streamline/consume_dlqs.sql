{{ config(
    materialized = 'view',
    tags = ['streamline_view'],
    post_hook = if_data_call_function(
      func = "{{this.schema}}.udf_streamline_produce_dlqs()",
      target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT 
    $1 as QUEUE_NAME
FROM VALUES (NULL);