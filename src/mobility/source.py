"""Download público executado no Databricks para um Volume gerenciado."""
from pathlib import Path
import shutil
import tempfile
from urllib.request import Request, urlopen

from mobility.config import source_month

TRIP_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


class TripSourceReader:
    def __init__(self, timeout: int = 120):
        self.timeout = timeout

    @staticmethod
    def trip_filename(month: str) -> str:
        return f"yellow_tripdata_{source_month(month)}.parquet"

    def download(self, url: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Download to local disk first: Unity Catalog volumes do not support all
        # filesystem rename operations. No reader starts until staging succeeds.
        with tempfile.TemporaryDirectory() as temporary:
            staged = Path(temporary) / destination.name
            request = Request(url, headers={"User-Agent": "urban-mobility-academic/0.1"})
            with urlopen(request, timeout=self.timeout) as response, staged.open("wb") as output:
                shutil.copyfileobj(response, output)
            if staged.stat().st_size == 0:
                raise ValueError(f"Empty source file: {destination.name}")
            if destination.suffix == ".parquet":
                with staged.open("rb") as stream:
                    magic = stream.read(4)
                    stream.seek(-4, 2)
                    if magic != b"PAR1" or stream.read(4) != b"PAR1":
                        raise ValueError("Source is not a complete Parquet file")
            shutil.copyfile(staged, destination)
        return destination

    def download_month(self, month: str, destination: Path) -> tuple[Path, Path]:
        name = self.trip_filename(month)
        return (
            self.download(f"{TRIP_BASE}/{name}", destination / name),
            self.download(ZONE_URL, destination / "taxi_zone_lookup.csv"),
        )
