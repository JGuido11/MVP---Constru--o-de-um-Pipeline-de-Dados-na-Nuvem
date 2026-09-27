"""One source month per manual DAG run; max_active_runs serializes all writes."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task


with DAG(
    dag_id="urban_mobility",
    description="Stage → ingest → prepare → dbt; one monthly batch at a time",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params={"source_month": Param("2026-01", type="string", enum=["2026-01", "2026-02", "2026-03"])},
    tags=["mobility", "academic", "batch"],
) as dag:

    @task(execution_timeout=timedelta(minutes=30))
    def stage():
        from mobility.orchestration import config_from_env
        from mobility.source import stage_month
        context = get_current_context()
        return stage_month(context["params"]["source_month"], config_from_env())

    @task(execution_timeout=timedelta(hours=2, minutes=15))
    def ingest():
        from mobility.orchestration import run_remote_job
        context = get_current_context()
        return run_remote_job("ingestion", context["params"]["source_month"],
                              context["run_id"], context["ti"].try_number)

    @task(execution_timeout=timedelta(hours=2, minutes=15))
    def prepare():
        from mobility.orchestration import run_remote_job
        context = get_current_context()
        return run_remote_job("preparation", context["params"]["source_month"],
                              context["run_id"], context["ti"].try_number)

    @task(execution_timeout=timedelta(hours=2))
    def analytics():
        from mobility.orchestration import run_dbt
        context = get_current_context()
        return run_dbt(context["params"]["source_month"], context["run_id"])

    stage() >> ingest() >> prepare() >> analytics()
