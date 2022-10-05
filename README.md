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
### Variables

To control which external table environment a model references, as well as, whether a Stream is invoked at runtime using control variables:
* STREAMLINE_INVOKE_STREAMS
When True, invokes streamline on model run as normal
When False, NO-OP
* STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES
When True, uses DEV schema Streamline.Ethereum_DEV
When False, uses PROD schema Streamline.Ethereum

Default values are False

* Usage:
dbt run --var '{STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES:True, STREAMLINE_INVOKE_STREAMS}'  -m ...

### Resources:

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
* Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
* Find [dbt events](https://events.getdbt.com) near you
* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
