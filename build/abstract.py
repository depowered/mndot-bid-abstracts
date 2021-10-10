import requests
import re
from io import StringIO


base_url = 'http://transport.dot.state.mn.us/PostLetting/abstractCSV.aspx?ContractId='


class Abstract:
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

            # Split the response data by blank lines to divide into its three subtables
            blank_line_regex = r"(?:\r?\n){2,}"
            self.contract_bytestr, self.bid_bytestr, self.bidder_bytestr = re.split(
                blank_line_regex, self.response.text)

        except requests.exceptions.RequestException as e:
            raise SystemExit(e)


    def stream_contract_data(self):
        return StringIO(self.contract_bytestr)


    def stream_bid_data(self):
        return StringIO(self.bid_bytestr)


    def stream_bidder_data(self):
        return StringIO(self.bidder_bytestr)


    def __str__(self) -> str:
        return f'Contract ID: {self.contract_id}'