SHELL := /bin/bash

dbt-console: 
	docker-compose run dbt_console

refresh_package:
	rm -f package-lock.yml
	dbt clean
	dbt deps
.PHONY: dbt-console refresh_package

