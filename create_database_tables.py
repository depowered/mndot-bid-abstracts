from build.table import ItemTable
from data.model import Base, Session, engine, Item


# create all tables
Base.metadata.create_all(engine)

# populate Item table from csv
items_csv = 'data/2018_TrnsportItemList.csv'
item_table = ItemTable(items_csv)

with Session() as session:
    for record in item_table.records:
        if session.get(Item, ident=record.ItemID) is None:
            session.add(record)

    session.commit()