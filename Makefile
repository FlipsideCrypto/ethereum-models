DBT_TARGET ?= dev
RECEIPTS_BY_HASH_ENABLED ?= false

cleanup_time:
	@set -e; \
	rm -f package-lock.yml && dbt clean && dbt deps

deploy_gha_workflows_table:
	@set -e; \
	echo "Collecting workflow names..." ; \
	WORKFLOW_VALUES="" ; \
	for file in $$(find .github/workflows -name "*.yml" -type f); do \
		filename=$$(basename "$$file" .yml) ; \
		if [ -z "$$WORKFLOW_VALUES" ]; then \
			WORKFLOW_VALUES="('$$filename')" ; \
		else \
			WORKFLOW_VALUES="$$WORKFLOW_VALUES,('$$filename')" ; \
		fi ; \
	done ; \
	echo "Found workflows: $$WORKFLOW_VALUES" ; \
	dbt run-operation create_workflow_table --args "{\"workflow_values\": \"$$WORKFLOW_VALUES\"}" -t $(DBT_TARGET)

deploy_gha_tasks:
	@set -e; \
	make deploy_gha_workflows_table DBT_TARGET=$(DBT_TARGET); \
	dbt run -s livequery_models.deploy.marketplace.github --vars '{"UPDATE_UDFS_AND_SPS":True}' -t $(DBT_TARGET); \
	dbt run -m "fsc_evm,tag:gha_tasks" --full-refresh -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.create_gha_tasks --vars '{"RESUME_GHA_TASKS":True}' -t $(DBT_TARGET)

deploy_new_gha_tasks:
	@set -e; \
	make deploy_gha_workflows_table DBT_TARGET=$(DBT_TARGET); \
	dbt run -m "fsc_evm,tag:gha_tasks" --full-refresh -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.create_gha_tasks --vars '{"RESUME_GHA_TASKS":True}' -t $(DBT_TARGET)

deploy_livequery:
	@set -e; \
	dbt run-operation fsc_evm.drop_livequery_schemas --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run -m livequery_models.deploy.core --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.livequery_grants --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET)

deploy_chain_phase_1:
	@set -e; \
	dbt run -m livequery_models.deploy.core --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.livequery_grants --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.create_evm_streamline_udfs --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET); \
	dbt run-operation fsc_evm.call_sample_rpc_node -t $(DBT_TARGET); \
	if [ "$(DBT_TARGET)" != "prod" ]; then \
		if [ "$(RECEIPTS_BY_HASH_ENABLED)" = "true" ]; then \
			dbt run -m "fsc_evm,tag:phase_1" --exclude "fsc_evm,tag:receipts" --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true, "MAIN_SL_NEW_BUILD_ENABLED": true, "GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET); \
			dbt test -m "fsc_evm,tag:chainhead"; \
			dbt run -m "fsc_evm,tag:streamline,tag:core,tag:complete" "fsc_evm,tag:streamline,tag:core,tag:realtime" --exclude "fsc_evm,tag:receipts" "fsc_evm,tag:confirm_blocks" --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "STREAMLINE_INVOKE_STREAMS":True, "MAIN_SL_TESTING_LIMIT": 500}' -t $(DBT_TARGET); \
		else \
			dbt run -m "fsc_evm,tag:phase_1" --exclude "fsc_evm,tag:receipts_by_hash" --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true, "MAIN_SL_NEW_BUILD_ENABLED": true, "GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET); \
			dbt test -m "fsc_evm,tag:chainhead"; \
			dbt run -m "fsc_evm,tag:streamline,tag:core,tag:complete" "fsc_evm,tag:streamline,tag:core,tag:realtime" --exclude "fsc_evm,tag:receipts_by_hash" "fsc_evm,tag:confirm_blocks" --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "STREAMLINE_INVOKE_STREAMS":True, "MAIN_SL_TESTING_LIMIT": 500}' -t $(DBT_TARGET); \
		fi; \
	else \
		if [ "$(RECEIPTS_BY_HASH_ENABLED)" = "true" ]; then \
			dbt run -m "fsc_evm,tag:phase_1" --exclude "fsc_evm,tag:receipts" --full-refresh --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET); \
			dbt test -m "fsc_evm,tag:chainhead"; \
			dbt run -m "fsc_evm,tag:streamline,tag:core,tag:complete" "fsc_evm,tag:streamline,tag:core,tag:realtime" --exclude "fsc_evm,tag:receipts" "fsc_evm,tag:confirm_blocks" --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "STREAMLINE_INVOKE_STREAMS":True}' -t $(DBT_TARGET); \
		else \
			dbt run -m "fsc_evm,tag:phase_1" --exclude "fsc_evm,tag:receipts_by_hash" --full-refresh --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET); \
			dbt test -m "fsc_evm,tag:chainhead"; \
			dbt run -m "fsc_evm,tag:streamline,tag:core,tag:complete" "fsc_evm,tag:streamline,tag:core,tag:realtime" --exclude "fsc_evm,tag:receipts_by_hash" "fsc_evm,tag:confirm_blocks" --vars '{"MAIN_SL_NEW_BUILD_ENABLED": true, "STREAMLINE_INVOKE_STREAMS":True}' -t $(DBT_TARGET); \
		fi; \
	fi; \
	echo "# wait ~10 minutes"; \
	echo "# run deploy_chain_phase_2"

deploy_chain_phase_2:
	@set -e; \
	if [ "$(DBT_TARGET)" != "prod" ]; then \
		dbt run -m "fsc_evm,tag:phase_2" --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true, "GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_BRONZE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true, "GLOBAL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:streamline,tag:abis,tag:realtime" "fsc_evm,tag:streamline,tag:abis,tag:complete" --vars '{"STREAMLINE_INVOKE_STREAMS":True, "DECODER_SL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
	else \
		dbt run -m "fsc_evm,tag:phase_2" --full-refresh --vars '{"GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_BRONZE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true, "GLOBAL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:streamline,tag:abis,tag:realtime" "fsc_evm,tag:streamline,tag:abis,tag:complete" --vars '{"STREAMLINE_INVOKE_STREAMS":True, "DECODER_SL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
	fi; \
	echo "# wait ~10 minutes"; \
	echo "# run deploy_chain_phase_3"

deploy_chain_phase_3:
	@set -e; \
	if [ "$(DBT_TARGET)" != "prod" ]; then \
		dbt run -m "fsc_evm,tag:phase_2" --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:phase_3" --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true, "GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true, "GLOBAL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:silver,tag:abis" "fsc_evm,tag:streamline,tag:decoded_logs,tag:realtime" "fsc_evm,tag:streamline,tag:decoded_logs,tag:complete" --vars '{"STREAMLINE_INVOKE_STREAMS":True, "DECODER_SL_TESTING_LIMIT": 500}' -t $(DBT_TARGET); \
	else \
		dbt run -m "fsc_evm,tag:phase_2" -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:phase_3" --full-refresh --vars '{"GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true, "GLOBAL_NEW_BUILD_ENABLED": true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:silver,tag:abis" "fsc_evm,tag:streamline,tag:decoded_logs,tag:realtime" "fsc_evm,tag:streamline,tag:decoded_logs,tag:complete" --vars '{"STREAMLINE_INVOKE_STREAMS":True}' -t $(DBT_TARGET); \
	fi; \
	echo "# wait ~10 minutes"; \
	echo "# run deploy_chain_phase_4"

deploy_chain_phase_4:
	@set -e; \
	if [ "$(DBT_TARGET)" != "prod" ]; then \
		dbt run -m "fsc_evm,tag:phase_3" --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":true}' -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:phase_4" --full-refresh -t $(DBT_TARGET); \
	else \
		dbt run -m "fsc_evm,tag:phase_3" -t $(DBT_TARGET); \
		dbt run -m "fsc_evm,tag:phase_4" --full-refresh -t $(DBT_TARGET); \
		make deploy_gha_tasks DBT_TARGET=$(DBT_TARGET); \
	fi; \

.PHONY: cleanup_time deploy_gha_workflows_table deploy_gha_tasks deploy_new_gha_tasks deploy_livequery deploy_chain_phase_1 deploy_chain_phase_2 deploy_chain_phase_3 deploy_chain_phase_4