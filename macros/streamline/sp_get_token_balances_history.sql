{% macro create_sp_get_token_balances_history() %}
  {% set sql %}
  CREATE
  OR REPLACE PROCEDURE streamline.sp_get_token_balances_history() returns variant LANGUAGE SQL AS $$
DECLARE
  RESULT variant;
row_cnt INTEGER;
BEGIN
  row_cnt:= (
    SELECT
      COUNT(1)
    FROM
      {{ ref('streamline__token_balances_history') }}
  );
if (
    row_cnt > 0
  ) THEN RESULT:= (
    SELECT
      streamline.udf_get_token_balances_history()
  );
  ELSE RESULT:= NULL;
END if;
RETURN RESULT;
END;$$ {% endset %}
{% do run_query(sql) %}
{% endmacro %}
