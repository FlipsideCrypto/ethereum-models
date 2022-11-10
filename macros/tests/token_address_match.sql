{% test token_address_match(
        model,
        column_name
) %}

SELECT DISTINCT {{ column_name }}
FROM
    {{ model }}
WHERE 
    {{ column_name }} IS NOT NULL 
    AND (LEN({{ column_name }}) != 42 
    OR {{ column_name }} NOT ILIKE '0x%')

{% endtest %}