.PHONY: setup-oracle stop-oracle start-oracle dbt-debug dbt-build dbt-test

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
	export ORA_PYTHON_DRIVER_TYPE=thick && \
	export ORACLE_HOME=/Users/astropd/Projects/oracle/instantclient_23_3 && \
	dbt debug --profiles-dir ./elt --project-dir ./elt

dbt-build:
	export ORA_PYTHON_DRIVER_TYPE=thick && \
	export ORACLE_HOME=/Users/astropd/Projects/oracle/instantclient_23_3 && \
	dbt build --profiles-dir ./elt --project-dir ./elt

dbt-test:
	export ORA_PYTHON_DRIVER_TYPE=thick && \
	export ORACLE_HOME=/Users/astropd/Projects/oracle/instantclient_23_3 && \
	dbt test --profiles-dir ./elt --project-dir ./elt
