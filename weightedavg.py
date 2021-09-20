import pandas as pd
from build.db import Connection
from abc import ABC

class TransportList():
    '''Columns of the transport list from the Items table.'''

    df: pd.DataFrame
    query: str = '''
    SELECT Item.ItemID, Item.Description, Item.Unit
    FROM Item'''

    def __init__(self) -> None:
        with Connection() as conn:
            self.df = pd.read_sql(self.query, conn, index_col='ItemID')

class BidData():
    '''Bid data with functions for serving filtered dataframes.'''

    df: pd.DataFrame
    query: str = '''
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

    WHERE Contract.Year >= 2018'''

    def __init__(self) -> None:
        with Connection() as conn:
            self.df = pd.read_sql(self.query, conn)

    def get_df_by_year(self, year: int):
        '''Returns a dataframe filtered by year.'''

        filt = self.df['Year'] == year
        return self.df[filt].copy()

class WeightedAverage():
    '''Base class for calculating and exporting weighted average bid prices.'''

    df_in: pd.DataFrame
    df_out: pd.DataFrame
    years = [2021, 2020, 2019, 2018]
    where_clause: str 
    base_query: str = '''
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

    def __init__(self) -> None:
        self.where_clause = 'WHERE Contract.Year >= 2018'
        query = self.base_query + self.where_clause
        with Connection() as conn:
            self.df_in = pd.read_sql(query, conn)

        transport_list = TransportList()
        self.df_out = transport_list.df
        for year in self.years:
            self.append_year_to_df_out(year)

    def filter_by_bidder(self, df_in: pd.DataFrame, bidder: str):
        '''Returns a GroupBy object filtered by bidder and grouped by ItemID.
        Bidder options: "Engineer", "BidderID_0", "BidderID_1", "BidderID_2".'''
        
        column = bidder + '_TotalPrice'
        filt = df_in[column].notna() # filter df_in for specified bidder
        df_filt = df_in[filt] 

        return df_filt.groupby(['ItemID'])

    def calc_contract_occur(self, df_in: pd.DataFrame, bidder: str):
        '''Returns a series of the contract occurnace count for all items for a given bidder.
        Bidder options: "Engineer", "BidderID_0", "BidderID_1", "BidderID_2".'''
        
        column = bidder + '_TotalPrice'
        grp = self.filter_by_bidder(df_in, bidder)
        
        return grp[column].count()

    def calc_weighted_avg(self, df_in: pd.DataFrame, bidder: str):
        '''Returns the weighted average for all items for a given bidder.
        Bidder options: "Engineer", "BidderID_0", "BidderID_1", "BidderID_2".'''
        
        column = bidder + '_TotalPrice'
        grp = self.filter_by_bidder(df_in, bidder)
        
        return ( grp[column].sum() / grp['Quantity'].sum() ).round(2)

    def get_df_by_year(self, year):
        '''Returns a dataframe filtered by year.'''

        filt = self.df_in['Year'] == year
        return self.df_in[filt].copy()

    def append_year_to_df_out(self, year: int):
        '''Appends ContractOccurance and WeightedUnitPrice columns to df_out.'''

        bidders = ['Engineer', 'BidderID_0', 'BidderID_1', 'BidderID_2']
        df_year = self.get_df_by_year(year)

        for bidder in bidders:
            contract_occur_column = str(year) + '_' + bidder + '_ContractOccurance'
            weighted_uprice_column = str(year) + '_' + bidder + '_WeightedUnitPrice'
            
            self.df_out[contract_occur_column] = self.calc_contract_occur(df_year, bidder)
            self.df_out[weighted_uprice_column] = self.calc_weighted_avg(df_year, bidder)

    def to_csv(self, filename: str):
        self.df_out.to_csv(filename)

# wt_avg = WeightedAverage()
# print(wt_avg.df_out.info())
# wt_avg.to_csv('weighted_avg.csv')

# bid_data = BidData()
# bid_data_2021 = bid_data.get_df_by_year(2021)
# print(bid_data_2021.info())