from __future__ import annotations

import argparse
from collections.abc import Sequence

from parqlite.db import connect


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="parqlite")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ui_parser = subparsers.add_parser("ui", help="Open DuckDB UI")
    ui_parser.add_argument("path", help="parqlite root directory")

    args = parser.parse_args(argv)

    if args.command == "ui":
        db = connect(args.path)
        try:
            db.open_ui()
        finally:
            db.close()


if __name__ == "__main__":
    main()
