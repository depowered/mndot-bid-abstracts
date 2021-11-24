from data.model import Session, Item2020, Item2018
from sqlalchemy import select


def get_unique_id_set_2018(session: Session):
    '''Get a set of unique ItemIDs (int) for spec year 2018'''
    subq = (
        select(Item2018.ItemID_2018).
        where(Item2018.ItemID_2018 == Item2020.ItemID_2020)
    ).exists()

    statement = select(Item2018.ItemID_2018).where(~subq)
    result = session.execute(statement)

    unique_ids = set()
    for id in result.scalars():
        unique_ids.add(id)

    return unique_ids


def get_unique_id_set_2020(session: Session):
    '''Get a set of unique ItemIDs (int) for spec year 2020'''
    subq = (
        select(Item2020.ItemID_2020).
        where(Item2018.ItemID_2018 == Item2020.ItemID_2020)
    ).exists()

    statement = select(Item2020.ItemID_2020).where(~subq)
    result = session.execute(statement)

    unique_ids = set()
    for id in result.scalars():
        unique_ids.add(id)

    return unique_ids


class UniqueItemSet():
    '''Contains two sets, one for each spec year, of ItemIDs that only occur in
    that spec year.'''

    def __init__(self) -> None:
        with Session() as session:
            self.unique_ids_2020 = get_unique_id_set_2020(session)
            self.unique_ids_2018 = get_unique_id_set_2018(session)


    def exists(self, item_id: int, spec_year: int):
        if spec_year == 2020:
            return item_id in self.unique_ids_2020
        else:
            return item_id in self.unique_ids_2018


    def get_spec_year(self, item_id):
        if item_id in self.unique_ids_2018:
            return 2018
        else:
            return 2020

# Initialize an instance of UniqueItemSet to import into the modules that use it
unique_item_set = UniqueItemSet()