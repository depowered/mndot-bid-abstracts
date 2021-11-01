from abstract import AbstractData
from db import get_table_columns, Connection, Cursor
import pandas as pd
from abc import ABC, abstractmethod


#
# Format helper functions
#
def get_item_number_as_int(item_number: str):
    '''Convert ItemNumber string to int'''
    return int(item_number.replace('/', ''))


def price_to_float(price):
    '''Convert unit price string to float'''
    return float(price.strip().replace('$', '').replace(',', ''))


#
# Table classes for each subtable from an abstract with interfaces defined 
# by an abstract base class
#

class Table(ABC):

    @abstractmethod
    def __init__(self, abstract_data: AbstractData) -> None:
        pass


    @abstractmethod
    def create_output_df(self) -> pd.DataFrame:
        '''Creates a dataframe that is formatted to match the destination SQL table.'''
        pass


    def to_db(self):
        '''Inserts the sql formatted dataframe into the database.'''

        temp_table = 'Temp' + self.table
        with Connection() as conn:
            self.output_df.to_sql(
                name=temp_table, 
                con=conn, 
                if_exists='replace', 
                index=False
            )
        with Cursor() as cur:
            cur.execute(
                'INSERT OR IGNORE INTO Bidder SELECT * FROM TempBidder')


class BidderTable(Table):
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Bidder SQL table.'''

    def __init__(self, abstract_data: AbstractData) -> None:
        self.table: str = 'Bidder'
        self.abstract_data = abstract_data
        self.input_df = self.abstract_data.bidder_data
        self.output_df = self.create_output_df()


    def create_output_df(self):
        columns = get_table_columns(self.table)
        output_df = pd.DataFrame(columns=columns)

        output_df['BidderID'] = self.input_df['Bidder Number']
        output_df['Name'] = self.input_df['Bidder Name']

        return output_df


class ContractTable(Table):
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Contract SQL table.'''

    def __init__(self, abstract_data: AbstractData) -> None:
        self.table: str = 'Contract'
        self.abstract_data = abstract_data
        self.input_df = self.abstract_data.contract_data
        self.output_df = self.create_output_df()


    def create_output_df(self):
        columns = get_table_columns(self.table)
        output_df = pd.DataFrame(columns=columns, dtype='int')

        output_df['ContractID'] = self.input_df['Contract Id']
        output_df['Year'] = self.input_df['Letting Date'].apply(
            [lambda x: int(x[-4:])])
        output_df['LetDate'] = self.input_df['Letting Date']
        output_df['SPNumber'] = self.input_df['SP Number']
        output_df['District'] = self.input_df['District']
        output_df['County'] = self.input_df['County']
        output_df['BidderID_0'] = self.abstract_data.bidder_id_0
        
        # Check that other bidders exist before adding data
        if self.abstract_data.bidder_id_1:
            output_df['BidderID_1'] = self.abstract_data.bidder_id_1
        if self.abstract_data.bidder_id_2:
            output_df['BidderID_2'] = self.abstract_data.bidder_id_2

        return output_df


class BidTable(Table):
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Contract SQL table.'''

    def __init__(self, abstract_data: AbstractData) -> None:
        self.table: str = 'Bid'
        self.abstract_data = abstract_data
        self.input_df = self.abstract_data.bid_data
        self.output_df = self.create_output_df()


    def get_unique_id(self, item_number: str):
        '''Generate a unique id for each item bid by concatenating ContractID and ItemNumber, 
        removing the '/' and casting to int'''
        unique_id = str(self.abstract_data.contract_id) + item_number.replace('/', '')
        return int(unique_id)


    def create_output_df(self):
        columns = get_table_columns(self.table)
        output_df = pd.DataFrame(columns=columns)

        output_df['BidID'] = self.input_df['ItemNumber'].apply(
            self.get_unique_id
        )
        output_df['ContractID'] = self.abstract_data.contract_id
        output_df['ItemID'] = self.input_df['ItemNumber'].apply(
            get_item_number_as_int
        )
        output_df['Quantity'] = self.input_df['Quantity']
        output_df['Engineer_UnitPrice'] = self.input_df['Engineers (Unit Price)']
        output_df['Engineer_TotalPrice'] = self.input_df['Engineers (Extended Amount)']
        output_df['BidderID_0_UnitPrice'] = self.input_df.iloc[:, 10].apply(
            price_to_float
        )
        output_df['BidderID_0_TotalPrice'] = self.input_df.iloc[:, 11].apply(
            price_to_float
        )
        
        # Check that other bidders exist before adding data
        if self.abstract_data.bidder_id_1:
            output_df['BidderID_1_UnitPrice'] = self.input_df.iloc[:, 12].apply(
                price_to_float
            )
            output_df['BidderID_1_TotalPrice'] = self.input_df.iloc[:, 13].apply(
                price_to_float
            )

        if self.abstract_data.bidder_id_2:
            output_df['BidderID_2_UnitPrice'] = self.input_df.iloc[:, 14].apply(
                price_to_float
            )
            output_df['BidderID_2_TotalPrice'] = self.input_df.iloc[:, 15].apply(
                price_to_float
            )

        return output_df


# Basic tests
# data = AbstractData(200131)

# bidder_table = BidderTable(abstract_data=data)
# print(bidder_table.output_df.head())

# contract_table = ContractTable(abstract_data=data)
# print(contract_table.output_df.head())

# bid_table = BidTable(abstract_data=data)
# print(bid_table.output_df.head())

# Insert to database
# bidder_table.to_db()
# contract_table.to_db()
# bid_table.to_db()
# print("Process complete.")