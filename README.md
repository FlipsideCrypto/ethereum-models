## Profile Set Up

#### Use the following within profiles.yml 

----

```yml
ethereum:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <ACCOUNT>
      role: <ROLE>
      user: <USERNAME>
      password: <PASSWORD>
      region: <REGION>
      database: ETHEREUM_DEV
      warehouse: <WAREHOUSE>
      schema: silver
      threads: 12
      client_session_keep_alive: False
      query_tag: <TAG>
    prod:
      type: snowflake
      account: <ACCOUNT>
      role: <ROLE>
      user: <USERNAME>
      password: <PASSWORD>
      region: <REGION>
      database: ETHEREUM
      warehouse: <WAREHOUSE>
      schema: silver
      threads: 12
      client_session_keep_alive: False
      query_tag: <TAG>
```
## Variables

To control which external table environment a model references, as well as, whether a Stream is invoked at runtime using control variables:
* STREAMLINE_INVOKE_STREAMS
  * When True, invokes streamline on model run as normal
  * When False, NO-OP
* STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES
  * When True, uses DEV schema Streamline.Ethereum_DEV
  * When False, uses PROD schema Streamline.Ethereum
  * Default values are False

  * Usage: `dbt run --var '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True, "STREAMLINE_INVOKE_STREAMS":True}'  --select ...`

To control the creation of UDF or SP macros with dbt run:
* UPDATE_UDFS_AND_SPS
  * Default values are False
  * When True, executes all macros included in the on-run-start hooks within dbt_project.yml on model run as normal
  * When False, none of the on-run-start macros are executed on model run

  * Usage: `dbt run --vars '{"UPDATE_UDFS_AND_SPS":True}' --select ...`

Use a variable to heal a model incrementally:
* HEAL_MODEL
  * Default is FALSE (Boolean)
  * When FALSE, logic will be negated
  * When TRUE, heal logic will apply
  * Include `heal` in model tags within the config block for inclusion in the `dbt_run_heal_models` workflow, e.g. `tags = 'heal'`

  * Usage: `dbt run --vars '{"HEAL_MODEL":True}' --select ...`

Use a variable to negate incremental logic:
* Example use case: reload records in a curated complete table without a full-refresh, such as `silver_bridge.complete_bridge_activity`:
* HEAL_MODELS
  * Default is an empty array []
  * When item is included in var array [], incremental logic will be skipped for that CTE / code block  
  * When item is not included in var array [] or does not match specified item in model, incremental logic will apply
  * Example set up: `{% if is_incremental() and 'axelar' not in var('HEAL_MODELS') %}`

  * Usage:
    * Single CTE: `dbt run --vars '{"HEAL_MODELS":"axelar"}' --select ...`
    * Multiple CTEs: `dbt run --vars '{"HEAL_MODELS":["axelar","across","celer_cbridge"]}' --select ...`

Use a variable to extend the incremental lookback period:
* LOOKBACK
  * Default is a string representing the specified time interval e.g. '12 hours', '7 days' etc.
  * Example set up: `SELECT MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'`

  * Usage: `dbt run --vars '{"LOOKBACK":"36 hours"}' --select ...`

## Applying Model Tags

### Database / Schema level tags

Database and schema tags are applied via the `add_database_or_schema_tags` macro.  These tags are inherited by their downstream objects.  To add/modify tags call the appropriate tag set function within the macro.

```
{{ set_database_tag_value('SOME_DATABASE_TAG_KEY','SOME_DATABASE_TAG_VALUE') }}
{{ set_schema_tag_value('SOME_SCHEMA_TAG_KEY','SOME_SCHEMA_TAG_VALUE') }}
```

### Model tags

To add/update a model's snowflake tags, add/modify the `meta` model property under `config`.  Only table level tags are supported at this time via DBT.

```
{{ config(
    ...,
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'SOME_PURPOSE'
            }
        }
    },
    ...
) }}
```

By default, model tags are pushed to Snowflake on each load. You can disable this by setting the `UPDATE_SNOWFLAKE_TAGS` project variable to `False` during a run.

```
dbt run --var '{"UPDATE_SNOWFLAKE_TAGS":False}' --select models/core/core__fact_blocks.sql
```

### Querying for existing tags on a model in snowflake

```
select *
from table(ethereum.information_schema.tag_references('ethereum.core.fact_blocks', 'table'));
```

### Resources:
* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
* Find [dbt events](https://events.getdbt.com) near you
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices