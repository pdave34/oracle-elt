.PHONY: setup-oracle stop-oracle start-oracle dbt-debug dbt-build dbt-test dbt-seed dbt-ls-seeds dagster-run

setup-oracle:
	docker run -d --name oracle-19c \
	-p 1521:1521 \
	-e ORACLE_PWD=Vaishali7? \
	-e ORACLE_PDB=pdave \
	-e ORACLE_SID=d7wob1d1 \
	-v /Users/astropd/Projects/oracle/oracle-19c/oradata/:/opt/oracle/oradata \
	oracle/database:19.3.0-ee 

start-oracle:
	docker start oracle-19c

stop-oracle:
	docker stop oracle-19c

dbt-debug:
	source .venv/bin/activate && \
	dbt debug --profiles-dir ./elt --project-dir ./elt

dbt-build:
	source .venv/bin/activate && \
	dbt build --profiles-dir ./elt --project-dir ./elt

dbt-test:
	source .venv/bin/activate && \
	dbt test --profiles-dir ./elt --project-dir ./elt

dbt-seed:
	source .venv/bin/activate && \
	dbt seed --profiles-dir ./elt --project-dir ./elt

dbt-ls-seeds:
	source .venv/bin/activate && \
	dbt ls --resource-type seed --profiles-dir ./elt --project-dir ./elt

dagster-run:
	dagster dev -f dagster_project/repository.py