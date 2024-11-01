SHELL := /bin/bash

dbt-console: 
	docker-compose run dbt_console

refresh_package:
	rm -f package-lock.yml
	dbt clean
	dbt deps
	dbt run-operation fsc_utils.create_evm_streamline_udfs --vars '{UPDATE_UDFS_AND_SPS: true}' --target dev-admin

realtime:
	dbt run -m models/streamline/silver/decoder/realtime/streamline__decode_traces_realtime.sql --vars '{"STREAMLINE_INVOKE_STREAMS":True,"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True"}' --target dev-admin
	dbt run -m models/streamline/bronze/decoder/bronze__streamline_decoded_traces.sql --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True}' --target dev-admin
	dbt run -m models/silver/core/silver__decoded_traces.sql

realtime_logs:
	dbt run -m models/streamline/silver/decoder/realtime/streamline__decode_logs_realtime.sql --vars '{"STREAMLINE_INVOKE_STREAMS":True,"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True"}' --target dev-admin
	dbt run -m models/streamline/bronze/decoder/bronze__streamline_decoded_logs.sql --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True}' --target dev-admin
	dbt run -m models/silver/core/silver__decoded_logs.sql

history:
	dbt run -m models/streamline/silver/decoder/history/traces/range_1/streamline__decode_traces_history_011667449_011706397.sql --vars '{"STREAMLINE_INVOKE_STREAMS":True,"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True"}' --target dev-admin
	dbt run -m models/streamline/bronze/decoder/bronze__streamline_decoded_traces.sql --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True}' --target dev-admin
	dbt run -m models/silver/core/silver__decoded_traces.sql

history_logs:
	dbt run -m models/streamline/silver/decoder/history/event_logs/range_0/streamline__decode_logs_history_016532020_016560020.sql --vars '{"STREAMLINE_INVOKE_STREAMS":True,"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True"}' --target dev-admin
	dbt run -m models/streamline/bronze/decoder/bronze__streamline_decoded_logs.sql --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True}' --target dev-admin
	dbt run -m models/silver/core/silver__decoded_logs.sql

load_new: 
	dbt run -m models/silver/core/silver__blocks.sql 
	dbt run -m models/silver/core/silver__transactions.sql 
	dbt run -m models/silver/core/silver__receipts.sql 
	dbt run -m models/silver/core/silver__logs.sql 
	dbt run -m models/silver/core/silver__traces.sql	

load_abi: 
	dbt run -m models/silver/core/silver__relevant_contracts.sql
	dbt run -m models/silver/core/silver__created_contracts.sql
	dbt run -m models/silver/abis --exclude models/silver/abis/event_logs

load_new_and_abi:
	make load_new
	make load_abi

.PHONY: dbt-console refresh_package

