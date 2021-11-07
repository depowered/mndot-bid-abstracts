import pandas as pd
from data.model import engine, Item, Bid, Contract
from sqlalchemy import select


def get_df_by_year(bid_df: pd.DataFrame, year: int) -> pd.DataFrame:
    '''Returns a dataframe filtered by year.'''
    filt = bid_df['Year'] == year
    return bid_df[filt].copy()


def filter_by_bidder(bid_year_df: pd.DataFrame, bidder_category: str):
    '''Returns a GroupBy object filtered by bidder and grouped by ItemID.
    Bidder options: "Engineer", "BidderID_0", "BidderID_1", "BidderID_2".'''

    column = bidder_category + '_TotalPrice'
    filt = bid_year_df[column].notna()  # filter df_in for specified bidder
    filt_df = bid_year_df[filt]

    return filt_df.groupby(['ItemID'])


def calc_contract_occur(bid_year_df: pd.DataFrame, bidder_category: str):
    '''Returns a series of the contract occurnace count for all items for a given bidder.
    Bidder options: "Engineer", "BidderID_0", "BidderID_1", "BidderID_2".'''

    column = bidder_category + '_TotalPrice'
    grp = filter_by_bidder(bid_year_df, bidder_category)

    return grp[column].count()


def calc_weighted_avg(bid_year_df: pd.DataFrame, bidder_category: str):
    '''Returns the weighted average for all items for a given bidder.
    Bidder options: "Engineer", "BidderID_0", "BidderID_1", "BidderID_2".'''

    column = bidder_category + '_TotalPrice'
    grp = filter_by_bidder(bid_year_df, bidder_category)

    return (grp[column].sum() / grp['Quantity'].sum()).round(2)


def get_weighted_avg_by_year(item_df: pd.DataFrame, bid_df: pd.DataFrame, year: int) -> pd.DataFrame:
    '''Returns dataframe of weighted average unit prices and contract occurrences
    for each bidder category ('Engineer', 'BidderID_0', 'BidderID_1', 'BidderID_2').'''

    bidder_category = ['Engineer', 'BidderID_0', 'BidderID_1', 'BidderID_2']
    bid_year_df = get_df_by_year(bid_df=bid_df, year=year)
    df = pd.DataFrame(index=item_df.index.to_series())

    for bidder in bidder_category:
        contract_occur_column = str(year) + '_' + bidder + '_ContractOccurrence'
        weighted_uprice_column = str(year) + '_' + bidder + '_WeightedUnitPrice'

        df[contract_occur_column] = calc_contract_occur(bid_year_df, bidder)
        df[weighted_uprice_column] = calc_weighted_avg(bid_year_df, bidder)

    return df


def create_weighted_avg_df(item_df: pd.DataFrame, bid_df: pd.DataFrame) -> pd.DataFrame:
    years = [2021, 2020, 2019, 2018]
    df = item_df.copy()

    for year in years:
        wt_avg_year_df = get_weighted_avg_by_year(
            item_df=item_df,
            bid_df=bid_df,
            year=year
        )
        df = df.join(wt_avg_year_df)
    return df


with engine.connect() as conn:
    # create item list dataframe
    item_select = select(Item.ItemID, Item.Description, Item.Unit)
    item_df = pd.read_sql(item_select, con=conn, index_col='ItemID')

    # create bid data dataframe
    bid_select = (
        select(
            Bid,
            Contract.Year,
            Contract.District,
            Contract.County,
        ).
        join(Contract, onclause=Bid.ContractID==Contract.ContractID)
    )
    bid_df = pd.read_sql(bid_select, con=conn)

# create weighted average dataframe
df = create_weighted_avg_df(item_df=item_df, bid_df=bid_df)

# export weighted average dataframe to csv
df.to_csv('Spec2018_weighted_average_all.csv')