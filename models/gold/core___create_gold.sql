{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'ddl_hash',
    merge_update_columns = [],
    )
}}
{% if execute %}
SELECT
$${{- generate_datashare_ddl() -}}$$ AS ddl,
md5(ddl) AS ddl_hash,
sysdate() as ddl_created_at
{% else %}
SELECT
null as ddl,
null as ddl_hash,
null as ddl_created_at
from dual limit 0
{% endif %}