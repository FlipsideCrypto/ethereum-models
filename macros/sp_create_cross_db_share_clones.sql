{% macro sp_create_cross_db_share_clones() %}
  {% set sql %}
  CREATE
  OR REPLACE PROCEDURE silver.sp_create_cross_db_share_clones() returns variant LANGUAGE SQL AS $$
BEGIN
    create or replace transient table silver.token_prices_hourly clone flipside_prod_db.ethereum.token_prices_hourly;
    create or replace transient table silver.dex_liquidity_pools clone flipside_prod_db.ethereum.dex_liquidity_pools;
    create or replace transient table silver.labels clone flipside_prod_db.silver_crosschain.address_labels;
END;$$ {% endset %}
{% do run_query(sql) %}
{% endmacro %}
