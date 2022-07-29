{% macro create_sp_get_contract_reads_realtime() %}
    {% set sql %}
    CREATE
    OR REPLACE PROCEDURE streamline.sp_get_contract_reads_realtime() returns variant LANGUAGE SQL AS $$
DECLARE
    RESULT variant;
row_cnt INTEGER;
BEGIN
    row_cnt:= (
        SELECT
            COUNT(1)
        FROM
            {{ ref('streamline__contract_reads_realtime') }}
    );
if (
        row_cnt > 0
    ) THEN RESULT:= (
        SELECT
            streamline.udf_get_contract_reads()
    );
    ELSE RESULT:= NULL;
END if;
RETURN RESULT;
END;$$ {% endset %}
{% do run_query(sql) %}
{% endmacro %}
