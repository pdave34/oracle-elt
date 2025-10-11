# Project Overview

This project is a data engineering pipeline that uses Dagster to orchestrate a dbt (Data Build Tool) project. The dbt project transforms data within an Oracle database. The entire environment, including the Oracle database, is managed using Docker.

**Key Technologies:**

*   **Orchestration:** Dagster
*   **Data Transformation:** dbt (Data Build Tool)
*   **Database:** Oracle
*   **Containerization:** Docker
*   **Language:** Python

**Architecture:**

1.  **Oracle Database:** An Oracle 19c database runs in a Docker container. Data is stored in the `oracle-19c/oradata` directory on the host machine.
2.  **dbt Project:** The `elt` directory contains a dbt project with models for transforming data in the Oracle database.
3.  **Dagster Orchestration:** The `dagster_project` directory contains a Dagster repository that defines assets, jobs, and schedules for running the dbt project. A Dagster webserver can be used to monitor and trigger the pipeline.

# Building and Running

This project uses a `Makefile` to simplify common tasks.

**1. Setting up the Oracle Database:**

To create and start the Oracle database container for the first time, run:

```bash
make setup-oracle
```

**2. Starting and Stopping the Oracle Database:**

To start the Oracle database container if it's already been created:

```bash
make start-oracle
```

To stop the container:

```bash
make stop-oracle
```

**3. Running dbt Commands:**

The `Makefile` provides helpers to run dbt commands with the correct environment variables.

*   **Test dbt connection:**

    ```bash
    make dbt-debug
    ```

*   **Build dbt models:**

    ```bash
    make dbt-build
    ```

*   **Test dbt models:**

    ```bash
    make dbt-test
    ```

**4. Running the Dagster Pipeline:**

To run the Dagster pipeline, you can use the Dagster UI.

First, install the Python dependencies:

```bash
pip install -e .
```

Then, start the Dagster webserver:

```bash
dagster-webserver -w dagster_project/repository.py
```

The webserver will be available at `http://localhost:3000`. From the UI, you can trigger the `dbt_build_job` and monitor the pipeline runs. The pipeline is also scheduled to run every 5 minutes.

# Development Conventions

*   **dbt Models:** dbt models are located in the `elt/models` directory. Follow dbt best practices for creating and documenting models.
*   **Dagster Assets:** Dagster assets are defined in `dagster_project/repository.py`.
*   **Environment Variables:** The `Makefile` sets the `ORA_PYTHON_DRIVER_TYPE` and `ORACLE_HOME` environment variables, which are required for dbt to connect to the Oracle database.
