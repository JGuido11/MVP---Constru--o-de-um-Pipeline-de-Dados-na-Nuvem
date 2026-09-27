import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from mobility.config import PipelineConfig, source_month
from mobility.source import TripSourceReader


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


if __name__ == "__main__":
    unittest.main()
