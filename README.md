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

## Bronze External Tables

Bronze external tables should be added to [bronze/models/sources.yml](bronze/models/sources.yml) in the bronze sub-project.  The following commands can be used to create the external tables as needed:

### Create all external tables

```sh
dbt run-operation --target dev stage_external_sources --vars "ext_full_refresh: true" --project-dir bronze
```

### Create single external table

```sh
dbt run-operation --target dev stage_external_sources --vars "ext_full_refresh: true" --args "select: ethereum_external_bronze.token_balances" --project-dir external_tables
```

### Resources:

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
* Find [dbt events](https://events.getdbt.com) near you
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
