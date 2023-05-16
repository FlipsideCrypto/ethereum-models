{% if execute %}
SELECT $${{- generate_datashare_ddl() -}}$$ AS ddl
{% else %}
SELECT null AS ddl from dual limit 0
{% endif %}