from db import Cursor
from abstract import AbstractData
from table import ContractTable, BidTable, BidderTable

'''Iterates through a list of contract IDs. Retrieves and processes the
data for each contract ID. Inserts the result to the database.'''


def to_log(error, abstract_id):
    '''Writes error to log file.'''
    with open('patch_process_log.txt', 'a') as f:
        f.write(f'Error processing Abstract ID: {abstract_id}\n')
        f.write(str(error))
        f.write('\n\n')


def get_abstract_list():
    '''Retrieves list of unprocessed abstracts'''
    with Cursor() as cur:
        cur.execute(
            'SELECT * FROM Abstract WHERE Processed = 0 ORDER BY AbstractID DESC')
        return cur.fetchall()


def set_processed(abstract_id: int):
    '''Sets the processed value of the given Contract ID to 1 (True).'''
    with Cursor() as cur:
        cur.execute(
            'UPDATE Abstract SET Processed = 1 WHERE AbstractID = ' + str(abstract_id))


def process_abstract(abstract_id: int):
    '''Processes abstract and inserts data into the associated SQL tables.'''
    data = AbstractData(abstract_id)
    contract_table = ContractTable(data)
    contract_table.to_db()
    bid_table = BidTable(data)
    bid_table.to_db()
    bidder_table = BidderTable(data)
    bidder_table.to_db()


def error_processing_abstract(abstract_id: int):
    with Cursor() as cur:
        cur.execute(
            'UPDATE Abstract SET Processed = -1 WHERE AbstractID = ' + str(abstract_id))


def main():
    abstract_lst = get_abstract_list()
    if len(abstract_lst) < 1:
        print('No abstracts to process.')

    count = 0
    for row in abstract_lst:
        abstract_id = row[0]

        print(f'Begin processing: {abstract_id}')

        try:
            process_abstract(abstract_id)
            set_processed(abstract_id)

            print(f'Finished processing: {abstract_id}')
            count = count + 1
            if count > 99:
                print('Retrieved 100 abstracts. Restart to retrieve more.')
                break

        except BaseException as error:
            to_log(error, abstract_id)
            error_processing_abstract(abstract_id)
            continue


if __name__ == '__main__':
    main()
