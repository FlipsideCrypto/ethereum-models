{% macro create_sp_get_rarible_nft_collections() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE streamline.sp_get_rarible_nft_collections() 
RETURNS variant 
LANGUAGE SQL 
AS 
$$
  DECLARE
    RESULT VARCHAR;
  BEGIN
    RESULT:= (
        SELECT
          streamline.udf_get_rarible_nft_collections()
      );
    RETURN RESULT;
  END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}