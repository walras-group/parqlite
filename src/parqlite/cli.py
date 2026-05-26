from __future__ import annotations

import argparse
from collections.abc import Sequence

from parqlite.db import connect
from parqlite.duckdb_backend import duckdb_describe_table_sql, duckdb_list_tables_sql


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="parqlite")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ui_parser = subparsers.add_parser("ui", help="Open DuckDB UI")
    ui_parser.add_argument("path", help="parqlite root directory")

    tables_parser = subparsers.add_parser("tables", help="List tables")
    tables_parser.add_argument("path", help="parqlite root directory")

    schema_parser = subparsers.add_parser("schema", help="Print a table schema")
    schema_parser.add_argument("path", help="parqlite root directory")
    schema_parser.add_argument("table", help="table name")

    sql_parser = subparsers.add_parser("sql", help="Open DuckDB or run a SQL query")
    sql_parser.add_argument("path", help="parqlite root directory")
    sql_parser.add_argument("query", nargs="?", help="SQL query")

    args = parser.parse_args(argv)

    with connect(args.path) as db:
        if args.command == "ui":
            db.open_ui()
        elif args.command == "tables":
            db.open_shell(duckdb_list_tables_sql())
        elif args.command == "schema":
            db.open_shell(duckdb_describe_table_sql(args.table))
        elif args.command == "sql":
            db.open_shell(args.query)


if __name__ == "__main__":
    main()
