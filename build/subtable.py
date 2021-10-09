from abstract import BidAbstract
from db import get_table_columns
import pandas as pd
from io import StringIO


class ContractTable:
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Contract SQL table.'''

    def __init__(self, bid_ab: BidAbstract) -> None:
        self.table: str = 'Contract'
        self.bid_ab = bid_ab
        # create a input_df from the contract_bytestr
        self.input_df = self.get_input_df()
        # create a output_df formatted for insertion
        self.output_df = self.get_output_df()
        # push to database

    def get_input_df(self):
        return pd.read_csv(StringIO(self.bid_ab.contract_bytestr))

    def get_output_df(self):
        columns = get_table_columns(self.table)
        output_df = pd.DataFrame(columns=columns)
        return output_df

bid_ab = BidAbstract(200131)
contract_table = ContractTable(bid_ab=bid_ab)
print(contract_table.input_df.shape)
print(contract_table.output_df.shape)