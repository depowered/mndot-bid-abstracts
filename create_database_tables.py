from build.table import ItemTable
from data.model import Base, Item2020, Session, engine, Item2018


# create all tables
Base.metadata.create_all(engine)

# populate Item2018 table from csv
item2018_csv = 'data/2018_TrnsportItemList.csv'
item2018_table = ItemTable(item2018_csv, year=2018)

with Session() as session:
    for record in item2018_table.records:
        if session.get(Item2018, ident=record.ItemID_2018) is None:
            session.add(record)

    session.commit()

# populate Item2018 table from csv
item2020_csv = 'data/2020_TrnsportItemList.csv'
item2020_table = ItemTable(item2020_csv, year=2020)

with Session() as session:
    for record in item2020_table.records:
        if session.get(Item2020, ident=record.ItemID_2020) is None:
            session.add(record)

    session.commit()
