{% macro sp_run_load_nft_metadata() -%}
CREATE OR REPLACE PROCEDURE bronze.sp_run_load_nft_metadata() 
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
        bronze.nft_metadata_api_requests
    );
    if (
        row_cnt > 0
      ) THEN RESULT:= (
        SELECT
          bronze.udf_load_nft_metadata()
      );
      ELSE RESULT:= NULL;
    END if;
    RETURN RESULT;
  END;
$$
{%- endmacro %}
