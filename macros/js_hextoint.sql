{% macro js_hex_to_int() %}
    CREATE
    OR REPLACE FUNCTION silver_ethereum_2022.js_hex_to_int (
        s STRING
    ) returns DOUBLE LANGUAGE javascript AS 'if (S !== null) { yourNumber = parseInt(S, 16); } return yourNumber'
{% endmacro %}
