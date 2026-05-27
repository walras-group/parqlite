from __future__ import annotations

from pathlib import Path
from shutil import copyfileobj
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd

from parqlite import DEFAULT_RETENTION_PROPERTIES, connect, month


DATA_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = DATA_DIR / ".binance-data"
KLINES_TABLE_NAME = "binance.klines"
FUNDING_RATES_TABLE_NAME = "binance.funding_rates"
KLINE_DATA_URLS = (
    "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-02.zip",
    "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-03.zip",
    "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2026-04.zip",
)
FUNDING_RATE_DATA_URLS = (
    "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-02.zip",
    "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-03.zip",
    "https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2026-04.zip",
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

FUNDING_RATE_SCHEMA = {
    "calc_time": "timestamptz",
    "funding_interval_hours": "int",
    "last_funding_rate": "float",
}

def main() -> None:
    kline_zip_paths = _download_archives(KLINE_DATA_URLS)
    funding_rate_zip_paths = _download_archives(FUNDING_RATE_DATA_URLS)

    db = connect("./crypto")
    try:
        db.create_namespace("binance")
        db.create_table(
            KLINES_TABLE_NAME,
            schema=KLINE_SCHEMA,
            partition_by=[month("opentime")],
            properties=DEFAULT_RETENTION_PROPERTIES,
        )
        db.create_table(
            FUNDING_RATES_TABLE_NAME,
            schema=FUNDING_RATE_SCHEMA,
            partition_by=[month("calc_time")],
            properties=DEFAULT_RETENTION_PROPERTIES,
        )

        kline_rows = 0
        for zip_path in kline_zip_paths:
            dataframe = _read_klines_zip(zip_path)
            db.append(KLINES_TABLE_NAME, dataframe)
            kline_rows += len(dataframe)

        funding_rate_rows = 0
        for zip_path in funding_rate_zip_paths:
            dataframe = _read_funding_rates_zip(zip_path)
            db.append(FUNDING_RATES_TABLE_NAME, dataframe)
            funding_rate_rows += len(dataframe)

        print(f"imported {kline_rows} rows into {KLINES_TABLE_NAME}")
        print(f"imported {funding_rate_rows} rows into {FUNDING_RATES_TABLE_NAME}")
    finally:
        db.close()


def _download_archives(urls: tuple[str, ...]) -> list[Path]:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    zip_paths = []
    for url in urls:
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


def _read_funding_rates_zip(path: Path) -> pd.DataFrame:
    with ZipFile(path) as zip_file:
        csv_name = next(name for name in zip_file.namelist() if name.endswith(".csv"))
        with zip_file.open(csv_name) as csv_file:
            dataframe = pd.read_csv(csv_file)

    dataframe["calc_time"] = pd.to_datetime(
        (dataframe["calc_time"].astype("int64") // 1000) * 1000,
        unit="ms",
        utc=True,
    )
    return dataframe[list(FUNDING_RATE_SCHEMA)]


if __name__ == "__main__":
    main()
