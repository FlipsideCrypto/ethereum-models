{% macro sp_create_prod_clone(target_schema) -%}
    CREATE
    OR REPLACE PROCEDURE {{ target_schema }}.create_prod_clone(
        source_db_name STRING,
        destination_db_name STRING,
        role_name STRING
    ) returns BOOLEAN LANGUAGE javascript EXECUTE AS caller AS $$ snowflake.execute(
        { sqlText: `BEGIN TRANSACTION;` }
    );
try { snowflake.execute(
        { sqlText: `CREATE OR REPLACE DATABASE ${DESTINATION_DB_NAME} CLONE ${SOURCE_DB_NAME}` }
    );
snowflake.execute(
        { sqlText: `DROP SCHEMA IF EXISTS ${DESTINATION_DB_NAME}._INTERNAL` }
    );
    /* this only needs to be in prod */
    snowflake.execute(
        { sqlText: `GRANT USAGE ON DATABASE ${DESTINATION_DB_NAME} TO AWS_LAMBDA_ETHEREUM_API` }
    );
var existing_schemas = snowflake.execute(
        { sqlText: `SELECT table_schema FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE grantor IS NOT NULL GROUP BY 1 UNION SELECT 'PUBLIC';` }
    );
WHILE (existing_schemas.next()) { var schema = existing_schemas.getColumnValue(1) if (schema.toUpperCase() == 'BRONZE') { snowflake.execute(
        { sqlText: `GRANT USAGE ON STAGE ${DESTINATION_DB_NAME}.${schema}.ANALYTICS_EXTERNAL_TABLES TO ROLE AWS_LAMBDA_ETHEREUM_API;` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE TABLES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE AWS_LAMBDA_ETHEREUM_API;` }
    );
snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE AWS_LAMBDA_ETHEREUM_API;` }
    );
snowflake.execute(
        { sqlText: `REVOKE SELECT ON FUTURE TABLES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE DBT_CLOUD_ETHEREUM` }
    );
snowflake.execute(
        { sqlText: `GRANT SELECT ON FUTURE TABLES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE INTERNAL_DEV;` }
    );}
    ELSE { snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE DBT_CLOUD_ETHEREUM` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE PROCEDURES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE DBT_CLOUD_ETHEREUM` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE TABLES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE DBT_CLOUD_ETHEREUM` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE VIEWS IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE DBT_CLOUD_ETHEREUM` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE ${ROLE_NAME}` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE PROCEDURES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE ${ROLE_NAME}` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE TABLES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE ${ROLE_NAME}` }
    );
snowflake.execute(
        { sqlText: `REVOKE OWNERSHIP ON FUTURE VIEWS IN SCHEMA ${DESTINATION_DB_NAME}.${schema} FROM ROLE ${ROLE_NAME}` }
    );
snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON FUTURE FUNCTIONS IN SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE ${ROLE_NAME};` }
    );
snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON FUTURE PROCEDURES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE ${ROLE_NAME};` }
    );
snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE ${ROLE_NAME};` }
    );
snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON FUTURE VIEWS IN SCHEMA ${DESTINATION_DB_NAME}.${schema} TO ROLE ${ROLE_NAME};` }
    );}} var existing_tables = snowflake.execute(
        { sqlText: `SELECT table_schema, table_name FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE grantor IS NOT NULL GROUP BY 1,2;` }
    );
WHILE (existing_tables.next()) { var schema = existing_tables.getColumnValue(1) var table_name = existing_tables.getColumnValue(2) snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON TABLE ${DESTINATION_DB_NAME}.${schema}.${table_name} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;` }
    );} var existing_functions = snowflake.execute(
        { sqlText: `SELECT function_schema, function_name, concat('(',array_to_string(regexp_substr_all(argument_signature, 'VARCHAR|NUMBER|FLOAT|ARRAY|VARIANT|OBJECT|DOUBLE'),','),')') as argument_signature FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.FUNCTIONS;` }
    );
WHILE (existing_functions.next()) { var schema = existing_functions.getColumnValue(1) var function_name = existing_functions.getColumnValue(2) var argument_signature = existing_functions.getColumnValue(3) snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON FUNCTION ${DESTINATION_DB_NAME}.${schema}.${function_name}${argument_signature} to role ${ROLE_NAME} REVOKE CURRENT GRANTS;` }
    );} var existing_procedures = snowflake.execute(
        { sqlText: `SELECT procedure_schema, procedure_name, concat('(',array_to_string(regexp_substr_all(argument_signature, 'VARCHAR|NUMBER|FLOAT|ARRAY|VARIANT|OBJECT|DOUBLE'),','),')') as argument_signature FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.PROCEDURES;` }
    );
WHILE (existing_procedures.next()) { var schema = existing_procedures.getColumnValue(1) var procedure_name = existing_procedures.getColumnValue(2) var argument_signature = existing_procedures.getColumnValue(3) snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON PROCEDURE ${DESTINATION_DB_NAME}.${schema}.${procedure_name}${argument_signature} to role ${ROLE_NAME} REVOKE CURRENT GRANTS;` }
    );} var existing_procedures = snowflake.execute(
        { sqlText: `SELECT stage_schema, stage_name FROM ${DESTINATION_DB_NAME}.INFORMATION_SCHEMA.STAGES;` }
    );
snowflake.execute(
        { sqlText: `GRANT OWNERSHIP ON DATABASE ${DESTINATION_DB_NAME} TO ROLE ${ROLE_NAME} COPY CURRENT GRANTS;` }
    ) snowflake.execute(
        { sqlText: `COMMIT;` }
    );} catch (err) { snowflake.execute(
        { sqlText: `ROLLBACK;` }
    );
throw(err);} RETURN TRUE $$
{%- endmacro %}
