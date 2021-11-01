import pandas as pd
from build.db import Connection


class ItemList():
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

    def __init__(self, district: str = None, county: str = None) -> None:
        if district:
            self.query = self.query + ' AND Contract.District = "' + district.capitalize() + '"'
        elif county:
            self.query = self.query + ' AND Contract.County = "' + county.upper() + '"'
        else:
            pass  # use unmodified self.query

        with Connection() as conn:
            self.df = pd.read_sql(self.query, conn)

    def get_df_by_year(self, year: int) -> pd.DataFrame:
        '''Returns a dataframe filtered by year.'''

        filt = self.df['Year'] == year
        return self.df[filt].copy()


class WeightedAverage():
    '''Base class for calculating and exporting weighted average bid prices.'''

    df: pd.DataFrame
    bid_data: BidData
    item_list: ItemList

    def __init__(self, district: str = None, county: str = None) -> None:
        self.item_list = ItemList()
        years = [2021, 2020, 2019, 2018]

        if district:
            self.bid_data = BidData(district=district)
        elif county:
            self.bid_data = BidData(county=county)
        else:
            self.bid_data = BidData()

        self.df = self.item_list.df.copy()
        for year in years:
            self.append_year_to_df_out(year)

    def filter_by_bidder(self, df_in: pd.DataFrame, bidder: str):
        '''Returns a GroupBy object filtered by bidder and grouped by ItemID.
        Bidder options: "Engineer", "BidderID_0", "BidderID_1", "BidderID_2".'''

        column = bidder + '_TotalPrice'
        filt = df_in[column].notna()  # filter df_in for specified bidder
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

        return (grp[column].sum() / grp['Quantity'].sum()).round(2)

    def append_year_to_df_out(self, year: int):
        '''Appends ContractOccurance and WeightedUnitPrice columns to df_out.'''

        bidders = ['Engineer', 'BidderID_0', 'BidderID_1', 'BidderID_2']
        df_year = self.bid_data.get_df_by_year(year)

        for bidder in bidders:
            contract_occur_column = str(year) + '_' + bidder + '_ContractOccurance'
            weighted_uprice_column = str(year) + '_' + bidder + '_WeightedUnitPrice'

            self.df[contract_occur_column] = self.calc_contract_occur(
                df_year, bidder)
            self.df[weighted_uprice_column] = self.calc_weighted_avg(
                df_year, bidder)

    def to_csv(self, filename: str):
        self.df.to_csv(filename)


def main():
    # calculate and export unfiltered weighted averages
    wgt_avg_all = WeightedAverage()
    wgt_avg_all.to_csv('2018_weighted_avg_all.csv')

    # wgt_avg_d3 = WeightedAverage(district='Baxter')
    # wgt_avg_d3.to_csv('2018_weighted_avg_d3.csv')

    # wgt_avg_morrison = WeightedAverage(county='morrison')
    # wgt_avg_morrison.to_csv('2018_weighted_avg_morrison.csv')


if __name__ == '__main__':
    main()
