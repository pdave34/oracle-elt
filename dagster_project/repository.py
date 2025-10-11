from dagster import AssetExecutionContext, Definitions, ScheduleDefinition, define_asset_job, AssetSelection
from dagster_dbt import DbtCliResource, dbt_assets

# Define the path to your dbt project
DBT_PROJECT_DIR = "./elt"

# Create a DbtCliResource instance
dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)

# Define dbt assets
@dbt_assets(manifest=DBT_PROJECT_DIR + "/target/manifest.json")
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# Define a job that builds all dbt assets
dbt_build_job = define_asset_job(name="dbt_build_job", selection=AssetSelection.all())

# Define a schedule for the job (e.g., daily at midnight)
daily_dbt_schedule = ScheduleDefinition(
    job=dbt_build_job,
    cron_schedule="*/10 * * * *",  # every 10 minutes
)

# Define the repository
defs = Definitions(
    assets=[dbt_project_assets],
    resources={
        "dbt": dbt_resource,
    },
    jobs=[dbt_build_job],
    schedules=[daily_dbt_schedule],
)
