import pandas as pd
from build.db import Connection
from abc import ABC

transport_list_query = '''
SELECT Item.ItemID, Item.Description, item.Unit
FROM Item'''

with Connection() as conn:
    df_transport = pd.read_sql(transport_list_query, conn, index_col='ItemID')

class WeightedAverage():
    '''Base class for calculating and exporting weighted average bid prices.'''
    df: pd.DataFrame
    df_out: pd.DataFrame
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
            self.df = pd.read_sql(query, conn)

    @staticmethod
    def filter_by_bidder(df_in: pd.DataFrame, bidder: str):
        '''Returns a GroupBy object filtered by bidder and grouped by ItemID.
        Bidder options: "Engineer", "BidderID_0", "BidderID_1", "BidderID_2".'''
        
        column = bidder + '_TotalPrice'
        
        filt = df_in[column].notna() # filter df_in for specified bidder
        df_filt = df_in[filt] 
        return df_filt.groupby(['ItemID'])

    def to_csv(self, filename: str):
        self.df_out.to_csv(filename)

wt_avg = WeightedAverage()
print(wt_avg.df.info())