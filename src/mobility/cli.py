"""Small entry point for integration checks and manual recovery."""
import argparse
import json
from uuid import uuid4

from mobility.orchestration import config_from_env, run_dbt, run_remote_job
from mobility.source import stage_month


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["smoke", "stage", "ingestion", "preparation", "analytics"])
    parser.add_argument("--month", default="2026-01")
    parser.add_argument("--run-id", default=None)
    arguments = parser.parse_args()
    run_id = arguments.run_id or f"manual-{uuid4()}"
    if arguments.command == "smoke":
        run_remote_job("bootstrap", arguments.month, run_id, 1)
        result = run_dbt(arguments.month, run_id, smoke=True)
    elif arguments.command == "stage":
        result = stage_month(arguments.month, config_from_env())
    elif arguments.command == "analytics":
        result = run_dbt(arguments.month, run_id)
    else:
        result = run_remote_job(arguments.command, arguments.month, run_id, 1)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
