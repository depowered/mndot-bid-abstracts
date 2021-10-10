import requests
import re
from io import StringIO


base_url = 'http://transport.dot.state.mn.us/PostLetting/abstractCSV.aspx?ContractId='


class Abstract:
    '''Context manager for retrieving and streaming web hosted bid abstract data'''

    def __init__(self, contract_id: int) -> None:
        self.contract_id = contract_id


    def __enter__(self):
        self.request_data()
        return self


    def __exit__(self, type, value, traceback):
        self = None


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
        '''Streams contract data for loading into pandas.read_csv() function.'''
        return StringIO(self.contract_bytestr)


    def stream_bid_data(self):
        '''Streams contract data for loading into pandas.read_csv() function.'''
        return StringIO(self.bid_bytestr)


    def stream_bidder_data(self):
        '''Streams contract data for loading into pandas.read_csv() function.'''
        return StringIO(self.bidder_bytestr)