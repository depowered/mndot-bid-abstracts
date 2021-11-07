from sqlalchemy import select, update
from build.abstract import AbstractData
from build.table import BidTable, BidderTable, ContractTable
from data.model import Bid, Bidder, Session, Abstract, Contract


def get_unprocessed_abstract_ids(session: Session) -> list:
    '''Retrieves a list of all Abstract IDs where Processed == "NO"'''
    statement = select(Abstract.AbstractID).where(Abstract.Processed=='NO')
    result = session.execute(statement)

    unprocessed_abstract_ids = []
    for id in result.scalars():
        unprocessed_abstract_ids.append(id)

    return unprocessed_abstract_ids


with Session() as session:

    unprocessed_abstract_ids = get_unprocessed_abstract_ids(session)

    for abstract_id in unprocessed_abstract_ids[:5]: # Set to only process 5 for testing purposes
        print(f'{abstract_id}')
        print('   Retrieving data')
        try:
            # Create table objects 
            abstract_data = AbstractData(abstract_id)
            contract_table = ContractTable(abstract_data)
            bid_table = BidTable(abstract_data)
            bidder_table = BidderTable(abstract_data)
            print('   Retrieval complete')

            # Insert table objects into database
            # Uses session.get() to avoid inserting duplicate records
            print('   Loading into database')
            for record in contract_table.records:
                if session.get(Contract, ident=record.ContractID) is None:
                    session.add(record)
            for record in bid_table.records:
                if session.get(Bid, ident=record.BidID) is None:
                    session.add(record)
            for record in bidder_table.records:
                if session.get(Bidder, ident=record.BidderID) is None:
                    session.add(record)
            print('   Loading complete')
            
            # Update processed to "YES"
            statement = (
                update(Abstract).
                where(Abstract.AbstractID==abstract_id).
                values(Processed="YES")
            )
            session.execute(statement)
            session.commit()
            print('   Processing complete')

        except BaseException as error:
            print(f'{abstract_id}: {error.__class__}: {error}')
            statement = (
                update(Abstract).
                where(Abstract.AbstractID==abstract_id).
                values(Processed="ERROR")
            )
            session.execute(statement)
            session.commit()