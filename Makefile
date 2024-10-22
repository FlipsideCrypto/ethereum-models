SHELL := /bin/bash

dbt-console: 
	docker-compose run dbt_console

refresh_package:
	rm -f package-lock.yml
	dbt clean
	dbt deps

realtime:
	dbt run -m models/streamline/silver/decoder/realtime/streamline__decode_traces_realtime.sql --vars '{"STREAMLINE_INVOKE_STREAMS":True,"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True"}' --target dev-admin
	dbt run -m models/streamline/bronze/decoder/bronze__streamline_decoded_traces.sql --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True}' --target dev-admin
	dbt run -m models/silver/core/silver__decoded_traces.sql

history:
	dbt run -m models/streamline/silver/decoder/history/traces/range_3/streamline__decode_traces_history_016944645_016980513.sql --vars '{"STREAMLINE_INVOKE_STREAMS":True,"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True"}' --target dev-admin
	dbt run -m models/streamline/bronze/decoder/bronze__streamline_decoded_traces.sql --full-refresh --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES":True}' --target dev-admin
	dbt run -m models/silver/core/silver__decoded_traces.sql

load_new: 
	dbt run -m models/silver/core/silver__blocks.sql 
	dbt run -m models/silver/core/silver__transactions.sql 
	dbt run -m models/silver/core/silver__receipts.sql 
	dbt run -m models/silver/core/silver__logs.sql 
	dbt run -m models/silver/core/silver__traces.sql	

.PHONY: dbt-console refresh_package

