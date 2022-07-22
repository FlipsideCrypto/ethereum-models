{% macro create_sp_get_rarible_nft_metadata() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE streamline.sp_get_rarible_nft_metadata() 
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
        streamline.rarible_nft_metadata
    );
    if (
        row_cnt > 0
      ) THEN RESULT:= (
        SELECT
          streamline.udf_get_rarible_nft_metadata()
      );
      ELSE RESULT:= NULL;
    END if;
    RETURN RESULT;
  END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}