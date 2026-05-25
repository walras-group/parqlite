from __future__ import annotations

from pathlib import Path
from shutil import copyfileobj
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd

from parquetdb import connect, month


DATA_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = DATA_DIR / ".binance-data"
TABLE_NAME = "binance.klines"
DATA_URLS = (
    "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-02.zip",
    "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-03.zip",
    "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-04.zip",
)

KLINE_SCHEMA = {
    "opentime": "timestamptz",
    "symbol": "string",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "volume": "double",
    "closetime": "timestamptz",
    "quote_volume": "double",
    "count": "int",
    "taker_buy_volume": "double",
    "taker_buy_quote_volume": "double",
    "ignore": "int",
}

METADATA_RETENTION_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": True,
    "write.metadata.previous-versions-max": 3,
}


def main() -> None:
    zip_paths = _download_klines()

    db = connect("./crypto")
    try:
        db.create_namespace("binance")
        db.create_table(
            TABLE_NAME,
            schema=KLINE_SCHEMA,
            partition_by=[month("opentime")],
            properties=METADATA_RETENTION_PROPERTIES,
        )

        rows = 0
        for zip_path in zip_paths:
            dataframe = _read_klines_zip(zip_path)
            db.append(TABLE_NAME, dataframe)
            rows += len(dataframe)

        print(f"imported {rows} rows into {TABLE_NAME}")
    finally:
        db.close()


def _download_klines() -> list[Path]:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    zip_paths = []
    for url in DATA_URLS:
        zip_path = DOWNLOAD_DIR / url.rsplit("/", 1)[1]
        if not zip_path.exists():
            part_path = zip_path.with_suffix(f"{zip_path.suffix}.part")
            print(f"downloading {url}")
            with urlopen(url) as response, part_path.open("wb") as file:
                copyfileobj(response, file)
            part_path.replace(zip_path)
        zip_paths.append(zip_path)

    return zip_paths


def _read_klines_zip(path: Path) -> pd.DataFrame:
    with ZipFile(path) as zip_file:
        csv_name = next(name for name in zip_file.namelist() if name.endswith(".csv"))
        with zip_file.open(csv_name) as csv_file:
            dataframe = pd.read_csv(csv_file)

    dataframe = dataframe.rename(
        columns={
            "open_time": "opentime",
            "close_time": "closetime",
        }
    )
    dataframe["opentime"] = pd.to_datetime(
        dataframe["opentime"],
        unit="ms",
        utc=True,
    )
    dataframe["closetime"] = pd.to_datetime(
        dataframe["closetime"],
        unit="ms",
        utc=True,
    )
    dataframe.insert(1, "symbol", path.stem.split("-", 1)[0])
    return dataframe[list(KLINE_SCHEMA)]


if __name__ == "__main__":
    main()
