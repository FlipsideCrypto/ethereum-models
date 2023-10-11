SHELL := /bin/bash

# set default target
DBT_TARGET ?= dev
AWS_LAMBDA_ROLE ?= aws_lambda_cosmos_ethereum

dbt-console: 
	docker-compose run dbt_console

.PHONY: dbt-console

sl-api:
	dbt run-operation create_aws_ethereum_api \
	--profile ethereum \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

udfs:
	dbt run-operation create_udfs \
	--vars '{"UPDATE_UDFS_AND_SPS":True}' \
	--profile ethereum \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

streamline: sl-api udfs 
