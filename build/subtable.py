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

    def get_bidder_count(self):
        return self.output_df.shape[0]

class ContractTable: # Relies on data from the bidder table in the previous implementation
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Contract SQL table.'''

    def __init__(self, abstract: BidAbstract) -> None:
        self.table: str = 'Contract'
        self.abstract = abstract
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
        # TODO: fill BidderID columns
        return output_df

    # TODO: implement to_db function



bid_ab = BidAbstract(200131)
contract_table = ContractTable(bid_ab=bid_ab)
print(contract_table.input_df.shape)
print(contract_table.output_df.head())

bidder_table = BidderTable(bid_ab)
print(bidder_table.output_df.head())
print(bidder_table.get_bidder_count())