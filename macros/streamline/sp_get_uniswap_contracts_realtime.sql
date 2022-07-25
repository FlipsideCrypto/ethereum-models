{% macro create_sp_get_token_balances_realtime() %}
  {% set sql %}
  CREATE
  OR REPLACE PROCEDURE streamline.sp_get_token_balances_realtime() returns variant LANGUAGE SQL AS $$
DECLARE
  RESULT variant;
row_cnt INTEGER;
BEGIN
  row_cnt:= (
    SELECT
      COUNT(1)
    FROM
      {{ ref('streamline__uniswap_contracts_realtime') }}
  );
if (
    row_cnt > 0
  ) THEN RESULT:= (
    SELECT
      streamline.udf_get_token_balances()
  );
  ELSE RESULT:= NULL;
END if;
RETURN RESULT;
END;$$ {% endset %}
{% do run_query(sql) %}
{% endmacro %}
