from __future__ import annotations

import argparse
from collections.abc import Sequence

from parquetdb.db import connect


def open_ui(path: str) -> None:
    db = connect(path)
    try:
        db.open_ui()
    finally:
        db.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Open a parquetdb database in DuckDB UI."
    )
    parser.add_argument("path", help="parquetdb root directory")
    args = parser.parse_args(argv)

    open_ui(args.path)


if __name__ == "__main__":
    main()
