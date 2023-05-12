{% if execute %}
SELECT $${{- get_all_view_ddl() -}}$$ AS ddl
{% else %}
SELECT null AS ddl from dual limit 0
{% endif %}