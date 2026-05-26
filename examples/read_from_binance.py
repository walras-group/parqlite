from parqlite import connect, snapshot_id


def main():
    db = connect("./crypto")

    tables = db.tables()
    print(tables)

    db.sql("SET TimeZone='UTC'")

    df = db.sql(query="select * from binance.klines order by opentime").df()

    print(df)

    snapshots = db.snapshots(table="binance.klines")

    print("len snapshots: ", len(snapshots))

    df = db.sql(
        query="select * from binance.klines order by opentime",
        at={"binance.klines": snapshot_id(snapshots[0].snapshot_id)},
    ).df()

    print(df)

    df = db.sql(
        query="select * from binance.klines order by opentime",
        at={"binance.klines": snapshot_id(snapshots[1].snapshot_id)},
    ).df()

    print(df)

    df = db.sql(
        query="select * from binance.klines order by opentime",
        at={"binance.klines": snapshot_id(snapshots[2].snapshot_id)},
    ).df()

    print(df)


if __name__ == "__main__":
    main()
