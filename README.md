# Urban Mobility Analytics

A service-oriented batch pipeline using **Databricks, Airflow, PySpark and dbt Core**.
It analyzes NYC yellow taxi activity, recorded fares and route durations for January–March 2026.

**Completion target: September 20, 2026. Submission: September 24, 2026.**

## Start here

Para testar o reprocessamento, instalar dbt no Workspace e construir gold, siga [REPROCESSAMENTO_DBT_GOLD.md](docs/REPROCESSAMENTO_DBT_GOLD.md).

Para importar os notebooks bronze e silver separados e configurar `source_month`, siga [NOTEBOOKS_AUTOMACAO.md](docs/NOTEBOOKS_AUTOMACAO.md).

For the browser-based, Databricks-only route, follow [DATABRICKS_WALKTHROUGH.md](docs/DATABRICKS_WALKTHROUGH.md).
That route uses Databricks Jobs for orchestration; Airflow remains the external-orchestration option below.

1. [Download the four source files and understand bronze](docs/DATA_SOURCES.md).
2. [Configure the local environment and Databricks](docs/SETUP.md).
3. [Understand contracts, quality rules and table definitions](docs/ARCHITECTURE.md).
4. [Collect execution evidence and write the report](docs/REPORT.md).

```text
Airflow on WSL/Linux (one active monthly DAG run)
    stage public files into a Databricks managed volume
      → Databricks ingestion job → bronze Delta tables
      → Databricks preparation job (PySpark) → silver + rejected rows
      → local dbt Core → gold tables + tests + documentation
```

The Databricks job definitions use serverless compute. Credentials, SQL warehouse
HTTP path and deployed job IDs must be configured locally before a real run.
No live execution or findings are claimed until the workspace checks pass.

## Main code

| Directory | Responsibility |
| --- | --- |
| `src/mobility` | Source reader, configuration, Delta storage, processing classes and orchestration helpers |
| `dags` | Airflow workflow, default success-only dependencies |
| `notebooks` | Databricks job entry points and acceptance demonstration |
| `dbt` | SQL models, tests, analytical queries and profiles |
| `tests` | Local contract, source and orchestration tests |

The fact table preserves source-record multiplicity: TLC provides no reliable unique
trip ID. Reprocessing replaces a source month instead of deduplicating similar trips.
This is an academic deployment with local Airflow, not a production availability claim.

## Local checks

```bash
python -m pip install -e .
python -m unittest discover -s tests -v
python -m compileall -q src dags notebooks
```

Databricks acceptance checks are in `notebooks/acceptance.py`; they use separate
`mobility_acceptance_*` schemas. See the setup guide before executing them.
