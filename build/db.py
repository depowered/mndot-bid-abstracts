import sqlite3
import csv

'''
Interface for managing the database.
'''

DATABASE = 'data/abstractdb.sqlite'


class Connection():
    '''Database connection object for pandas to_sql and read_sql methods'''

    def __init__(self, file: str = DATABASE):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


class Cursor():
    '''
    Context manager for database connections.
    Use in a 'with' block for all SQL statement executions.
    '''

    def __init__(self, file: str = DATABASE):
        self.file = file

    def __enter__(self):
        self.conn = sqlite3.connect(self.file)
        return self.conn.cursor()

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()


def get_table_names():
    '''Returns a list of table names.'''
    with Cursor() as cur:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        return cur.fetchall()


def drop_table():
    # Fetch and print the list of tables
    table_names = get_table_names()
    for i, name in enumerate(table_names):
        print(f'{i}: {name[0]}')

    # Select a table to drop
    print('Select a table to drop by entering its index:')
    print('(Enter -1 to exit function without droping table.)')
    inp = int(input())
    if inp == -1:
        print('Exiting function without dropping table.')
        return
    elif inp >= len(table_names):
        print('Input out of range. Exiting function without dropping table.')
        return

    # Confirm drop table
    table_to_drop = table_names[inp][0]
    print(f'Confirm drop table {table_to_drop} by entering "DELETE":')
    inp = input()
    if inp != "DELETE":
        print('Exiting function without dropping table.')
        return

    # Drop table
    else:
        sql_stmt = 'DROP TABLE IF EXISTS ' + table_to_drop
        with Cursor() as cur:
            cur.execute(sql_stmt)
            print(f'{table_to_drop} DROPPED!')


def create_item_table():
    '''Create Item table.'''
    with Cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Item (
            ItemID INTEGER NOT NULL PRIMARY KEY UNIQUE,
            SpecCode TEXT,
            UnitCode TEXT,
            ItemCode TEXT,
            Description TEXT,
            Unit TEXT
        )''')
    print('Item table CREATED!')


def csv_to_item_table(csvfile: str = 'INPUT/2020_TrnsportItemList.csv'):
    '''Populate item table from Tr*nsport List csv.'''

    with Cursor() as cur:
        with open(csvfile, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                spec_code = row['Item Number'][:4]  # string
                unit_code = row['Item Number'][5:8]  # string
                item_code = row['Item Number'][9:]  # string
                item_id = int(spec_code + unit_code + item_code)
                description = row['Long Description']
                unit = row['Unit Name']

                cur.execute('''
                INSERT OR IGNORE INTO Item
                (ItemID, SpecCode, UnitCode, ItemCode, Description, Unit) 
                VALUES ( ?, ?, ?, ?, ?, ? )''',
                (item_id, spec_code, unit_code, item_code, description, unit))

    print('Item table POPULATED!')


def create_abstract_table():
    '''Create Abstract table.'''
    with Cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Abstract (
            AbstractID INTEGER NOT NULL PRIMARY KEY UNIQUE,
            Year INTEGER,
            Processed INTEGER
        )''')
    print('Abstract table CREATED!')


def create_contract_table():
    '''Create Contract table.'''
    with Cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Contract (
            ContractID INTEGER NOT NULL PRIMARY KEY UNIQUE,
            Year INTEGER,
            LetDate DATE,
            SPNumber TEXT,
            District TEXT,
            County TEXT,
            BidderID_0 INTEGER,
            BidderID_1 INTEGER,
            BidderID_2 INTEGER
        )''')
    print('Contract table CREATED!')


def create_bid_table():
    '''Create Bid table.'''
    with Cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Bid (
            BidID INTEGER NOT NULL PRIMARY KEY UNIQUE,
            ContractID INTEGER,
            ItemID INTEGER,
            Quantity FLOAT,
            Engineer_UnitPrice FLOAT,
            Engineer_TotalPrice FLOAT,
            BidderID_0_UnitPrice FLOAT,
            BidderID_0_TotalPrice FLOAT,
            BidderID_1_UnitPrice FLOAT,
            BidderID_1_TotalPrice FLOAT,
            BidderID_2_UnitPrice FLOAT,
            BidderID_2_TotalPrice FLOAT
            )''')
    print('Bid table CREATED!')


def create_bidder_table():
    '''Create Bidder table'''
    with Cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS Bidder (
            BidderID INTEGER NOT NULL PRIMARY KEY UNIQUE,
            Name TEXT
        )''')
    print('Bidder table CREATED!')


def create_all_tables():
    '''Create ALL tables.'''
    create_item_table()
    create_abstract_table()
    create_contract_table()
    create_bid_table()
    create_bidder_table()


def query_all():
    '''Returns SQL statement for selecting bids for all years and all locations.'''
    sql_query = '''
    SELECT 
    Contract.Year, Contract.District, Contract.County, Bid.ContractID, 
    Bid.ItemID, Item.Description, Item.Unit, Bid.Quantity, 
    Bid.Engineer_UnitPrice, Bid.Engineer_TotalPrice, 
    Bid.BidderID_0_UnitPrice, Bid.BidderID_0_TotalPrice, 
    Bid.BidderID_1_UnitPrice, Bid.BidderID_1_TotalPrice,
    Bid.BidderID_2_UnitPrice, Bid.BidderID_2_TotalPrice

    FROM Bid 

    JOIN Contract ON Bid.ContractID = Contract.ContractID,
    Item ON Bid.ItemID = Item.ItemID

    '''

    return sql_query


def query_by_district(district: str):
    '''Returns SQL statement for selecting bids for all years in a select district.
    District names are in Proper case.'''
    sql_query = query_all() + 'WHERE Contract.District = "' + \
        district.capitalize() + '"'
    return sql_query


def query_by_county(county: str):
    '''Returns SQL statement for selecting bids for all years in a select county.
    County names are in all upper case.'''
    sql_query = query_all() + 'WHERE Contract.County = "' + county.upper() + '"'
    return sql_query


def query_by_year(year: int):
    '''Returns SQL statement for selecting bids for a select year.'''
    sql_query = query_all() + 'WHERE Contract.Year = ' + str(year)
    return sql_query
