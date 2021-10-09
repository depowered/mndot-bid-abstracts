import requests
import re
from io import StringIO
import pandas as pd
from abc import ABC
from db import Cursor, Connection
from dataclasses import dataclass

base_url = 'http://transport.dot.state.mn.us/PostLetting/abstractCSV.aspx?ContractId='


class BidAbstract:
    '''Represents a bid BidAbstract for a construction contract.
    Requests csv data from MnDOT's BidAbstracts for Awarded Jobs web app. 
    Splits the csv data into contract, bid, and bidder sub-tables for analysis in pandas.
    '''

    def __init__(self, contract_id: int) -> None:
        self.contract_id = contract_id
        self.request_data()

    def request_data(self):
        '''Requests data from web app and splits into subtables bytestrings.'''
        try:
            self.response = requests.get(base_url + str(self.contract_id))
            # Raise a RequestException if there is an error with the response
            self.response.raise_for_status()

            # Split the resonse data by blank lines to divide into its three subtables
            blank_line_regex = r"(?:\r?\n){2,}"
            self.contract_bytestr, self.bid_bytestr, self.bidder_bytestr = re.split(
                blank_line_regex, self.response.text)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)

    def __str__(self) -> str:
        return f'Contract ID: {self.contract_id}'


class AbstractTable(ABC):
    '''Base class for BidAbstract sub tables.'''
    ab: BidAbstract
    raw_df: pd.DataFrame
    sql_df: pd.DataFrame
    temp_db_table: str
    insert_statement: str

    def __init__(self, ab: BidAbstract) -> None:
        self.ab = ab
        self.get_sql_df()

    def get_sql_df(self):
        pass

    def to_db(self):
        pass

    def __repr__(self) -> str:
        # Class: classname, Rows: rowcount, Columns: columncount
        return f'Class: {str(self.__class__).split(".")[1][:-2]}, Rows: {self.sql_df.shape[0]}, Columns: {self.sql_df.shape[1]}'


class ContractTable(AbstractTable):
    '''Transforms the raw contract dataframe into a format the matches the destination SQL table schema.'''

    def __init__(self, ab: BidAbstract) -> None:
        super().__init__(ab)

    def get_sql_df(self):
        '''Builds dataframe for inserting into Contract SQL table.'''

        # Determine number of bidders represented in the bidders subtable
        num_bidders = self.ab.bidder_df.shape[0]

        # Intialize an empty df to store data for Contract SQL table
        self.sql_df = pd.DataFrame()

        # Fill table
        self.sql_df['ContractID'] = self.ab.contract_df['Contract Id']
        self.sql_df['Year'] = self.ab.contract_df['Letting Date'].apply(
            [lambda x: int(x[-4:])])
        self.sql_df['LetDate'] = self.ab.contract_df['Letting Date']
        self.sql_df['SPNumber'] = self.ab.contract_df['SP Number']
        self.sql_df['District'] = self.ab.contract_df['District']
        self.sql_df['County'] = self.ab.contract_df['County']
        self.sql_df['BidderID_0'] = self.ab.bidder_df['Bidder Number'][0]

        # Verify that there are second and third lowest bidders
        if num_bidders > 1:
            self.sql_df['BidderID_1'] = self.ab.bidder_df['Bidder Number'][1]
        else:
            # Fill Series with NaN
            self.sql_df['BidderID_1'] = pd.Series(dtype='int')
        if num_bidders > 2:
            self.sql_df['BidderID_2'] = self.ab.bidder_df['Bidder Number'][2]
        else:
            # Fill Series with NaN
            self.sql_df['BidderID_2'] = pd.Series(dtype='int')

    def to_db(self):
        '''Inserts the sql formatted dataframe into the database.'''

        with Connection() as conn:
            self.sql_df.to_sql('TempContract', conn,
                               if_exists='replace', index=False)
        with Cursor() as cur:
            cur.execute(
                'INSERT OR IGNORE INTO Contract SELECT * FROM TempContract')


class BidTable(AbstractTable):
    '''Transforms the raw bid dataframe into a format the matches the destination SQL table schema.'''

    def __init__(self, ab: BidAbstract) -> None:
        super().__init__(ab)

    @staticmethod
    def price_to_float(price):
        '''Convert unit price string to float'''
        return float(price.strip().replace('$', '').replace(',', ''))

    @staticmethod
    def item_str_to_int(item_str):
        '''Convert ItemNumber string to int'''
        return int(item_str.replace('/', ''))

    def get_unique_id(self, item_str):
        '''Generate a unique bid id by concatenating ContractID and ItemNumber, removing the '/' and casting to int'''
        unique_id = str(self.ab.contract_id) + item_str.replace('/', '')
        return int(unique_id)

    def get_sql_df(self):
        '''Builds dataframe for inserting into Bid SQL table.'''

        # Used to verify presence of 2nd and 3rd lowest bidders
        num_columns = self.ab.bid_df.shape[1]
        # Intialize an empty df to store data for Bid SQL table
        self.sql_df = pd.DataFrame()

        # Fill table
        self.sql_df['BidID'] = self.ab.bid_df['ItemNumber'].apply(
            self.get_unique_id)
        self.sql_df['ContractID'] = self.ab.bid_df['ContractId']
        self.sql_df['ItemID'] = self.ab.bid_df['ItemNumber'].apply(
            self.item_str_to_int)
        self.sql_df['Quantity'] = self.ab.bid_df['Quantity']
        self.sql_df['Engineer_UnitPrice'] = self.ab.bid_df['Engineers (Unit Price)']
        self.sql_df['Engineer_TotalPrice'] = self.ab.bid_df['Engineers (Extended Amount)']

        # Selects the lowest bidder unit prices and item totals
        self.sql_df['BidderID_0_UnitPrice'] = self.ab.bid_df.iloc[:, 10].apply(
            self.price_to_float)
        self.sql_df['BidderID_0_TotalPrice'] = self.ab.bid_df.iloc[:, 11].apply(
            self.price_to_float)

        # Verify that there is a second lowest bidder
        if num_columns > 12:
            self.sql_df['BidderID_1_UnitPrice'] = self.ab.bid_df.iloc[:, 12].apply(
                self.price_to_float)
            self.sql_df['BidderID_1_TotalPrice'] = self.ab.bid_df.iloc[:, 13].apply(
                self.price_to_float)
        else:  # Fill both Series with NaN
            self.sql_df['BidderID_1_UnitPrice'] = pd.Series(dtype='float')
            self.sql_df['BidderID_1_TotalPrice'] = pd.Series(dtype='float')

        # Verify that there is a thrid lowest bidder
        if num_columns > 14:
            self.sql_df['BidderID_2_UnitPrice'] = self.ab.bid_df.iloc[:, 14].apply(
                self.price_to_float)
            self.sql_df['BidderID_2_TotalPrice'] = self.ab.bid_df.iloc[:, 15].apply(
                self.price_to_float)
        else:  # Fill both Series with NaN
            self.sql_df['BidderID_2_UnitPrice'] = pd.Series(dtype='float')
            self.sql_df['BidderID_2_TotalPrice'] = pd.Series(dtype='float')

    def to_db(self):
        '''Inserts the sql formatted dataframe into the database.'''

        with Connection() as conn:
            self.sql_df.to_sql(
                'TempBid', conn, if_exists='replace', index=False)
        with Cursor() as cur:
            cur.execute('INSERT OR IGNORE INTO Bid SELECT * FROM TempBid')


class BidderTable(AbstractTable):
    '''Transforms the raw bidder dataframe into a format the matches the destination SQL table schema.'''
    ab: BidAbstract
    sql_df: pd.DataFrame

    def __init__(self, ab: BidAbstract) -> None:
        super().__init__(ab)

    def get_sql_df(self):
        '''Builds dataframe for inserting into Bidder SQL table.'''

        # Initialize an empty df to store data for Bidder SQL table
        self.sql_df = pd.DataFrame()

        # Fill table
        self.sql_df['id'] = self.ab.bidder_df['Bidder Number']
        self.sql_df['name'] = self.ab.bidder_df['Bidder Name']

    def to_db(self):
        '''Inserts the sql formatted dataframe into the database.'''

        with Connection() as conn:
            self.sql_df.to_sql('TempBidder', conn,
                               if_exists='replace', index=False)
        with Cursor() as cur:
            cur.execute(
                'INSERT OR IGNORE INTO Bidder SELECT * FROM TempBidder')

# basic tests
ab = BidAbstract(200131)
print(ab)

# contract = ContractTable(ab)
# print(contract)
# contract.to_db()

# bid = BidTable(ab)
# print(bid)
# bid.to_db()

# bidder = BidderTable(ab)
# print(bidder)
# bidder.to_db()
