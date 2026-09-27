import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mobility.config import PipelineConfig, source_month
from mobility.orchestration import run_dbt, run_remote_job
from mobility.source import TripSourceReader, stage_month


class ConfigurationTests(unittest.TestCase):
    def test_month_rejects_malformed_and_injection(self):
        for value in ("2026-13", "2026-00", "25-01", "2026-1", "2026-01' OR 1=1", "../2026-01"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                source_month(value)
        self.assertEqual(source_month("2026-01"), "2026-01")

    def test_identifiers_cannot_inject_sql_or_paths(self):
        for value in ("foo.bar", "../foo", "foo; DROP TABLE trips", ""):
            with self.subTest(value=value), self.assertRaises(ValueError):
                PipelineConfig(catalog=value)

    def test_layer_is_validated(self):
        config = PipelineConfig("workspace", "mobility")
        self.assertEqual(config.table("bronze", "yellow_trips"), "workspace.mobility_bronze.yellow_trips")
        with self.assertRaises(ValueError):
            config.schema("untrusted")


class SourceTests(unittest.TestCase):
    def test_incomplete_parquet_does_not_replace_existing_download(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "trips.parquet"
            destination.write_bytes(b"PAR1originalPAR1")
            with patch("mobility.source.urlopen", return_value=io.BytesIO(b"PAR1truncated")):
                with self.assertRaises(ValueError):
                    TripSourceReader().download("https://example.invalid", destination)
            self.assertEqual(destination.read_bytes(), b"PAR1originalPAR1")

    def test_complete_parquet_is_preserved_byte_for_byte(self):
        payload = b"PAR1examplebytesPAR1"
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "trips.parquet"
            with patch("mobility.source.urlopen", return_value=io.BytesIO(payload)):
                TripSourceReader().download("https://example.invalid", destination)
            self.assertEqual(destination.read_bytes(), payload)

    def test_empty_download_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch("mobility.source.urlopen", return_value=io.BytesIO(b"")):
                with self.assertRaises(ValueError):
                    TripSourceReader().download("https://example.invalid", Path(directory) / "zones.csv")

    def test_staging_uploads_both_files_to_correct_month(self):
        client = MagicMock()

        def files(month, directory):
            trip = directory / f"yellow_tripdata_{month}.parquet"
            zones = directory / "taxi_zone_lookup.csv"
            trip.write_bytes(b"PAR1testPAR1")
            zones.write_text("LocationID,Zone\n1,Test\n")
            return trip, zones

        with patch.object(TripSourceReader, "download_month", side_effect=files):
            result = stage_month("2026-01", PipelineConfig(), client)
        self.assertEqual(client.files.upload.call_count, 2)
        destinations = [call.args[0] for call in client.files.upload.call_args_list]
        self.assertTrue(all(path.startswith("/Volumes/workspace/mobility_bronze/landing/2026-01/") for path in destinations))
        self.assertEqual(result["source_month"], "2026-01")


class OrchestrationTests(unittest.TestCase):
    @patch.dict(os.environ, {"MOBILITY_PREPARATION_JOB_ID": "123"})
    def test_remote_failure_propagates_and_retry_has_new_token(self):
        client = MagicMock()
        client.jobs.run_now.return_value.result.side_effect = RuntimeError("remote failure")
        for attempt in (1, 1, 2):
            with self.assertRaisesRegex(RuntimeError, "remote failure"):
                run_remote_job("preparation", "2026-01", "run-1", attempt, client)
        tokens = [call.kwargs["idempotency_token"] for call in client.jobs.run_now.call_args_list]
        self.assertEqual(tokens[0], tokens[1])
        self.assertNotEqual(tokens[1], tokens[2])

    @patch.dict(os.environ, {"MOBILITY_PREPARATION_JOB_ID": "123"})
    def test_timeout_cancels_remote_run_before_retry(self):
        client = MagicMock()
        client.jobs.run_now.return_value.run_id = 456
        client.jobs.run_now.return_value.result.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            run_remote_job("preparation", "2026-01", "run-1", 1, client)
        client.jobs.cancel_run_and_wait.assert_called_once_with(run_id=456)

    def test_failed_readiness_gate_prevents_dbt_build(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / "dbt"
            executable.touch()
            environment = {"MOBILITY_PROJECT_ROOT": directory, "DBT_EXECUTABLE": str(executable)}
            with patch.dict(os.environ, environment), patch("mobility.orchestration.dbt_environment", return_value={}):
                with patch("mobility.orchestration.subprocess.run", side_effect=subprocess.CalledProcessError(1, "dbt")) as process:
                    with self.assertRaises(subprocess.CalledProcessError):
                        run_dbt("2026-01", "run-1")
            self.assertEqual(process.call_count, 1)
            self.assertIn("assert_ready", process.call_args.args[0])

    def test_success_runs_gate_build_and_docs_without_shell(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / "dbt"
            executable.touch()
            with patch.dict(os.environ, {"MOBILITY_PROJECT_ROOT": directory, "DBT_EXECUTABLE": str(executable)}):
                with patch("mobility.orchestration.dbt_environment", return_value={}), patch("mobility.orchestration.subprocess.run") as process:
                    result = run_dbt("2026-01", "a/run:id")
            self.assertEqual(process.call_count, 3)
            self.assertIn("build", process.call_args_list[1].args[0])
            self.assertIn("docs", process.call_args_list[2].args[0])
            self.assertEqual(result["status"], "SUCCESS")
            self.assertTrue(all(not call.kwargs.get("shell", False) for call in process.call_args_list))


if __name__ == "__main__":
    unittest.main()
