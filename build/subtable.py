from abstract import BidAbstract
from db import get_table_columns
import pandas as pd
from io import StringIO


class BidderTable:
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Bidder SQL table.'''

    def __init__(self, bid_ab: BidAbstract) -> None:
        self.table: str = 'Bidder'
        self.bid_ab = bid_ab
        self.input_df = self.create_input_df()
        self.output_df = self.create_output_df()


    def create_input_df(self):
        return pd.read_csv(StringIO(self.bid_ab.bidder_bytestr))


    def create_output_df(self):
        columns = get_table_columns(self.table)
        output_df = pd.DataFrame(columns=columns)

        output_df['BidderID'] = self.input_df['Bidder Number']
        output_df['Name'] = self.input_df['Bidder Name']

        return output_df


    def to_db(self):
        raise NotImplemented


    def get_bidder_count(self):
        return self.output_df.shape[0]

class ContractTable: # Relies on data from the bidder table in the previous implementation
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Contract SQL table.'''

    def __init__(self, abstract: BidAbstract, bidder_table: BidderTable) -> None:
        self.table: str = 'Contract'
        self.abstract = abstract
        self.bidder_table = bidder_table
        self.input_df = self.create_input_df()
        self.output_df = self.create_output_df()


    def create_input_df(self):
        return pd.read_csv(StringIO(self.abstract.contract_bytestr))


    def create_output_df(self):
        columns = get_table_columns(self.table)
        output_df = pd.DataFrame(columns=columns)

        output_df['ContractID'] = self.input_df['Contract Id']
        output_df['Year'] = self.input_df['Letting Date'].apply(
            [lambda x: int(x[-4:])])
        output_df['LetDate'] = self.input_df['Letting Date']
        output_df['SPNumber'] = self.input_df['SP Number']
        output_df['District'] = self.input_df['District']
        output_df['County'] = self.input_df['County']
        output_df['BidderID_0'] = self.bidder_table.output_df['BidderID'][0]
        
        if self.bidder_table.get_bidder_count() > 1:
            output_df['BidderID_1'] = self.bidder_table.output_df['BidderID'][1]
        if self.bidder_table.get_bidder_count() > 2:
            output_df['BidderID_2'] = self.bidder_table.output_df['BidderID'][2]

        return output_df


    def to_db(self):
        raise NotImplemented

class BidTable:
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Contract SQL table.'''

    def __init__(self, abstract: BidAbstract, contract_table: ContractTable) -> None:
        self.table: str = 'Bid'
        self.abstract = abstract
        self.contract_table = bidder_table
        self.input_df = self.create_input_df()
        self.output_df = self.create_output_df()


    def create_input_df(self):
        return pd.read_csv(StringIO(self.abstract.bid_bytestr))


    def get_unique_id(self, item_number: str):
        '''Generate a unique bid id by concatenating ContractID and ItemNumber, 
        removing the '/' and casting to int'''
        unique_id = str(self.abstract.contract_id) + item_number.replace('/', '')
        return int(unique_id)


    @staticmethod
    def get_item_number_as_int(item_number: str):
        '''Convert ItemNumber string to int'''
        return int(item_number.replace('/', ''))


    @staticmethod
    def price_to_float(price):
        '''Convert unit price string to float'''
        return float(price.strip().replace('$', '').replace(',', ''))


    def create_output_df(self):
        columns = get_table_columns(self.table)
        output_df = pd.DataFrame(columns=columns)

        output_df['BidID'] = self.input_df['ItemNumber'].apply(
            self.get_unique_id
        )
        output_df['ContractID'] = self.abstract.contract_id
        output_df['ItemID'] = self.input_df['ItemNumber'].apply(
            self.get_item_number_as_int
        )
        output_df['Quantity'] = self.input_df['Quantity']
        output_df['Engineer_UnitPrice'] = self.input_df['Engineers (Unit Price)']
        output_df['Engineer_TotalPrice'] = self.input_df['Engineers (Extended Amount)']
        output_df['BidderID_0_UnitPrice'] = self.input_df.iloc[:, 10].apply(
            self.price_to_float)
        output_df['BidderID_0_TotalPrice'] = self.input_df.iloc[:, 11].apply(
            self.price_to_float)
        
        # Check for second lowest bidder before adding data
        if self.input_df.shape[1] > 12:
            output_df['BidderID_1_UnitPrice'] = self.input_df.iloc[:, 12].apply(
                self.price_to_float)
            output_df['BidderID_1_TotalPrice'] = self.input_df.iloc[:, 13].apply(
                self.price_to_float)

        # Check for third lowest bidder before adding data
        if self.input_df.shape[1] > 14:
            output_df['BidderID_2_UnitPrice'] = self.input_df.iloc[:, 14].apply(
                self.price_to_float)
            output_df['BidderID_2_TotalPrice'] = self.input_df.iloc[:, 15].apply(
                self.price_to_float)

        return output_df


    def to_db(self):
        raise NotImplemented


# Basic tests
bid_ab = BidAbstract(200131)

bidder_table = BidderTable(bid_ab)
print(bidder_table.output_df.head())

contract_table = ContractTable(abstract=bid_ab, bidder_table=bidder_table)
print(contract_table.output_df.head())

bid_table = BidTable(abstract=bid_ab, contract_table=contract_table)
print(bid_table.output_df.head())