import requests
import re
from io import StringIO
import pandas as pd


base_url = 'http://transport.dot.state.mn.us/PostLetting/abstractCSV.aspx?ContractId='


class Abstract:
    '''Context manager for retrieving and streaming web hosted bid abstract data into the AbstractData class.'''

    def __init__(self, contract_id: int) -> None:
        self.contract_id = contract_id
        self.url = base_url + str(contract_id)


    def __enter__(self):
        self.request_data()
        return self


    def __exit__(self, type, value, traceback):
        self = None


    def request_data(self):
        '''Requests data from web app and splits into subtables bytestrings.'''
        try:
            self.response = requests.get(self.url)
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


class AbstractData:
    '''Serves input data to Table classes.'''

    def __init__(self, contract_id: int) -> None:
        self.contract_id = contract_id

        # Retrieve data from the web and load into dataframes
        with Abstract(self.contract_id) as ab:
            self.contract_data = pd.read_csv(ab.stream_contract_data())
            self.bid_data = pd.read_csv(ab.stream_bid_data())
            self.bidder_data = pd.read_csv(ab.stream_bidder_data())


    @property
    def bidder_count(self):
        return int((len(self.bid_data.columns) - 10) / 2)

    @property
    def bidder_id_0(self):
        return self.bidder_data['Bidder Number'][0]

    @property
    def bidder_id_1(self):
        if self.bidder_count > 1:
            return self.bidder_data['Bidder Number'][1]
        else:
            return None

    @property
    def bidder_id_2(self):
        if self.bidder_count > 2:
            return self.bidder_data['Bidder Number'][2]
        else:
            return None