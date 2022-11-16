{% macro sp_create_load_nft_metadata() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% set sql %}
    CREATE OR REPLACE PROCEDURE silver.sp_run_load_nft_metadata() 
    RETURNS variant 
    LANGUAGE SQL 
    AS 
    $$
      DECLARE
        RESULT VARCHAR;
        row_cnt INTEGER;
      BEGIN
        row_cnt:= (
          SELECT
            COUNT(1)
          FROM
            silver.nft_metadata_api_requests
        );
        if (
            row_cnt > 0
          ) THEN RESULT:= (
            SELECT
              silver.udf_load_nft_metadata()
          );
          ELSE RESULT:= NULL;
        END if;
        RETURN RESULT;
      END;
    $${% endset %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}
