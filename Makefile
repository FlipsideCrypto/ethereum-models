DBT_TARGET ?= dev

deploy_streamline_functions:
	rm -f package-lock.yml && dbt clean && dbt deps
	dbt run -s livequery_models.deploy.core --vars '{"UPDATE_UDFS_AND_SPS":True}' -t $(DBT_TARGET)
	dbt run-operation fsc_utils.create_evm_streamline_udfs --vars '{"UPDATE_UDFS_AND_SPS":True}' -t $(DBT_TARGET)

cleanup_time:
	rm -f package-lock.yml && dbt clean && dbt deps

deploy_streamline_tables:
	rm -f package-lock.yml && dbt clean && dbt deps
ifeq ($(findstring dev,$(DBT_TARGET)),dev)
	dbt run -m "fsc_evm,tag:bronze_external" --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True}' -t $(DBT_TARGET)
else
	dbt run -m "fsc_evm,tag:bronze_external" -t $(DBT_TARGET)
endif
	dbt run -m "fsc_evm,tag:streamline_core_complete" "fsc_evm,tag:streamline_core_realtime" "fsc_evm,tag:utils" --full-refresh -t $(DBT_TARGET)

deploy_streamline_requests:
	rm -f package-lock.yml && dbt clean && dbt deps
	dbt run -m "fsc_evm,tag:streamline_core_complete" "fsc_evm,tag:streamline_core_realtime" --vars '{"STREAMLINE_INVOKE_STREAMS":True}' -t $(DBT_TARGET)

deploy_github_actions:
	dbt run -s livequery_models.deploy.marketplace.github --vars '{"UPDATE_UDFS_AND_SPS":True}' -t $(DBT_TARGET)
	dbt seed -s github_actions__workflows -t $(DBT_TARGET)
	dbt run -m "fsc_evm,tag:gha_tasks" --full-refresh -t $(DBT_TARGET)
ifeq ($(findstring dev,$(DBT_TARGET)),dev)
	dbt run-operation fsc_utils.create_gha_tasks --vars '{"START_GHA_TASKS":False}' -t $(DBT_TARGET)
else
	dbt run-operation fsc_utils.create_gha_tasks --vars '{"START_GHA_TASKS":True}' -t $(DBT_TARGET)
endif

deploy_new_github_action:
	dbt seed -s github_actions__workflows -t $(DBT_TARGET)
	dbt run -m "fsc_evm,tag:gha_tasks" --full-refresh -t $(DBT_TARGET)
ifeq ($(findstring dev,$(DBT_TARGET)),dev)
	dbt run-operation fsc_utils.create_gha_tasks --vars '{"START_GHA_TASKS":False}' -t $(DBT_TARGET)
else
	dbt run-operation fsc_utils.create_gha_tasks --vars '{"START_GHA_TASKS":True}' -t $(DBT_TARGET)
endif

regular_incremental:
	dbt run -m "fsc_evm,tag:core" -t $(DBT_TARGET)

.PHONY: deploy_streamline_functions deploy_streamline_tables deploy_streamline_requests deploy_github_actions cleanup_time regular_incremental deploy_new_github_action