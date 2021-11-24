from build.abstract import AbstractData
import pandas as pd
from abc import ABC, abstractmethod
from data.model import Bid, Bidder, Contract, Item2018, Item2020
from typing import Union
from data.unique_items import unique_item_set


###########################
# Format helper functions
###########################

def get_item_number_as_int(item_number: str):
    '''Convert ItemNumber string to int'''
    return int(item_number.replace('/', '').replace('.', ''))


def price_to_float(price):
    '''Convert unit price string to float'''
    return float(price.strip().replace('$', '').replace(',', ''))


def get_unique_bid_id( item_number: str, contract_id: int):
    '''Generate a unique id for each item bid by concatenating ContractID and ItemNumber, 
    removing the '/' and casting to int'''
    unique_id = str(contract_id) + item_number.replace('/', '')
    return int(unique_id)


############################################################################
# Table classes for each subtable from an abstract with interfaces defined 
# by an abstract base class
############################################################################

class DataTable(ABC):

    @abstractmethod
    def __init__(self, input_data: Union[AbstractData, str]) -> None:
        pass


    @abstractmethod
    def create_output_df(self) -> pd.DataFrame:
        '''Creates a dataframe that is formatted to match the destination SQL table.'''
        pass


    def create_records(self):
        '''Creates a list of database model objects (records) from each row of the output_df.'''
        pass


class BidderTable(DataTable):
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Bidder SQL table.'''

    def __init__(self, input_data: AbstractData) -> None:
        self.table: str = 'Bidder'
        self.input_data = input_data
        self.input_df = self.input_data.bidder_data
        self.output_df = self.create_output_df()
        self.records = self.create_records()


    def create_output_df(self):
        columns = Bidder().__table__.columns.keys()
        output_df = pd.DataFrame(columns=columns)

        output_df['BidderID'] = self.input_df['Bidder Number']
        output_df['Name'] = self.input_df['Bidder Name']

        return output_df

    def create_records(self) -> list[Bidder]:
        db_records = []
        for row in self.output_df.itertuples(index=False, name='BidderTable'):
            record = Bidder(
                BidderID = row.BidderID,
                Name = row.Name
            )
            db_records.append(record)

        return db_records


class ContractTable(DataTable):
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Contract SQL table.'''

    def __init__(self, input_data: AbstractData) -> None:
        self.table: str = 'Contract'
        self.input_data = input_data
        self.input_df = self.input_data.contract_data
        self.output_df = self.create_output_df()
        self.records = self.create_records()


    def create_output_df(self):
        columns = Contract().__table__.columns.keys()
        output_df = pd.DataFrame(columns=columns, dtype='int')

        output_df['ContractID'] = self.input_df['Contract Id']
        output_df['Year'] = self.input_df['Letting Date'].apply(
            [lambda x: int(x[-4:])])
        output_df['LetDate'] = self.input_df['Letting Date']
        output_df['SPNumber'] = self.input_df['SP Number']
        output_df['District'] = self.input_df['District']
        output_df['County'] = self.input_df['County']
        output_df['BidderID_0'] = self.input_data.bidder_id_0
        
        # Check that other bidders exist before adding data
        if self.input_data.bidder_id_1:
            output_df['BidderID_1'] = self.input_data.bidder_id_1
        if self.input_data.bidder_id_2:
            output_df['BidderID_2'] = self.input_data.bidder_id_2

        return output_df


    def create_records(self) -> list[Contract]:
        db_records = []
        for row in self.output_df.itertuples(index=False, name='ContractTable'):
            record = Contract(
                ContractID = row.ContractID,
                Year = row.Year,
                LetDate = row.LetDate,
                SPNumber = row.SPNumber,
                District = row.District,
                County = row.County,
                BidderID_0 = row.BidderID_0,
                BidderID_1 = row.BidderID_1,
                BidderID_2 = row.BidderID_2
            )
            db_records.append(record)

        return db_records


class BidTable(DataTable):
    '''Transforms raw contract subtable data into a format that can be inserted 
    into the Contract SQL table.'''

    def __init__(self, input_data: AbstractData) -> None:
        self.table: str = 'Bid'
        self.input_data = input_data
        self.input_df = self.input_data.bid_data
        self.output_df = self.create_output_df()
        self.records = self.create_records()


    def create_output_df(self):
        columns = Bid().__table__.columns.keys()
        output_df = pd.DataFrame(columns=columns)

        output_df['BidID'] = self.input_df['ItemNumber'].apply(
            get_unique_bid_id, args=(self.input_data.contract_id, )
        )
        output_df['ContractID'] = self.input_data.contract_id
        output_df['ItemID'] = self.input_df['ItemNumber'].apply(
            get_item_number_as_int
        )
        output_df['SpecYear'] = self.input_df['ItemNumber'].apply(
            unique_item_set.get_spec_year
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
        if self.input_data.bidder_id_1:
            output_df['BidderID_1_UnitPrice'] = self.input_df.iloc[:, 12].apply(
                price_to_float
            )
            output_df['BidderID_1_TotalPrice'] = self.input_df.iloc[:, 13].apply(
                price_to_float
            )

        if self.input_data.bidder_id_2:
            output_df['BidderID_2_UnitPrice'] = self.input_df.iloc[:, 14].apply(
                price_to_float
            )
            output_df['BidderID_2_TotalPrice'] = self.input_df.iloc[:, 15].apply(
                price_to_float
            )

        return output_df


    def create_records(self) -> list[Bid]:
        db_records = []
        for row in self.output_df.itertuples(index=False, name='BidTable'):
            record = Bid(
                BidID = row.BidID,
                ContractID = row.ContractID,
                ItemID = row.ItemID,
                Quantity = row.Quantity,
                Engineer_UnitPrice = row.Engineer_UnitPrice,
                Engineer_TotalPrice = row.Engineer_TotalPrice,
                BidderID_0_UnitPrice = row.BidderID_0_UnitPrice,
                BidderID_0_TotalPrice = row.BidderID_0_TotalPrice,
                BidderID_1_UnitPrice = row.BidderID_1_UnitPrice,
                BidderID_1_TotalPrice = row.BidderID_1_TotalPrice,
                BidderID_2_UnitPrice = row.BidderID_2_UnitPrice,
                BidderID_2_TotalPrice = row.BidderID_2_TotalPrice
            )
            db_records.append(record)

        return db_records


class ItemTable(DataTable):
    def __init__(self, input_data: str, year: int) -> None:
        self.table: str = 'Item'
        self.year = year
        self.input_data = input_data
        self.input_df = pd.read_csv(self.input_data)
        self.output_df = self.create_output_df()
        self.records = self.create_records()


    def create_output_df(self) -> pd.DataFrame:
        columns = Item2018().__table__.columns.keys()
        output_df = pd.DataFrame(columns=columns)

        output_df['ItemID'] = self.input_df['Item Number'].apply(
            get_item_number_as_int
        )
        output_df['SpecCode'] = self.input_df['Item Number'].apply(
            lambda x: x[:4]
        )
        output_df['UnitCode'] = self.input_df['Item Number'].apply(
            lambda x: x[5:8]
        )
        output_df['ItemCode'] = self.input_df['Item Number'].apply(
            lambda x: x[9:]
        )
        output_df['Description'] = self.input_df['Long Description']
        output_df['Unit'] = self.input_df['Unit Name']

        return output_df


    def create_records_2018(self) -> list[Item2018]:
        db_records = []
        for row in self.output_df.itertuples(index=False, name='ItemTable'):
            record = Item2018(
                ItemID_2018 = row.ItemID,
                SpecCode_2018 = row.SpecCode,
                UnitCode_2018 = row.UnitCode,
                ItemCode_2018 = row.ItemCode,
                Description_2018 = row.Description,
                Unit_2018 = row.Unit
            )
            db_records.append(record)

        return db_records


    def create_records_2020(self) -> list[Item2020]:
        db_records = []
        for row in self.output_df.itertuples(index=False, name='ItemTable'):
            record = Item2020(
                ItemID_2020 = row.ItemID,
                SpecCode_2020 = row.SpecCode,
                UnitCode_2020 = row.UnitCode,
                ItemCode_2020 = row.ItemCode,
                Description_2020 = row.Description,
                Unit_2020 = row.Unit,
                Item2018_ID = None
            )
            db_records.append(record)

        return db_records


    def create_records(self):
        if self.year == 2020:
            return self.create_records_2020()
        elif self.year == 2018:
            return self.create_records_2018()
        else:
            raise ValueError