{% macro sp_create_cross_db_share_clones() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
      {% set sql %}
      CREATE
      OR REPLACE PROCEDURE silver.sp_create_cross_db_share_clones() returns variant LANGUAGE SQL AS $$
    BEGIN
        create or replace transient table silver.token_prices_hourly clone flipside_prod_db.ethereum.token_prices_hourly;
        create or replace transient table silver.dex_liquidity_pools clone flipside_prod_db.ethereum.dex_liquidity_pools;
    END;$$ {% endset %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}
