from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from typing import List


class AbstractIDScraper:
    '''Retrieves a list of abstract IDs from MnDOT's hosting site using selenium webdriver.'''
    def __init__(self, year: int = 2021, headless: bool = True) -> None:
        self.year = year
        self.headless = headless
        self.url = 'http://transport.dot.state.mn.us/PostLetting/Abstract.aspx'
        self.browser = self.get_browser()
        self.abstract_ids: List[int] = self.get_abstract_id_list()


    def get_browser(self):
        if self.headless == True:
            opts = Options()
            opts.headless = True
            browser = Firefox(options=opts)
        else:
            browser = Firefox()
        browser.get(self.url)
        return browser


    def get_abstract_id_list(self):
        '''Scrapes list of abstract IDs from webpage.'''
        # Select letting year
        element = self.browser.find_element_by_id('MainContent_drpLettingYear')
        select = Select(element)
        select.select_by_value(str(self.year))

        # Submit initial form
        element = self.browser.find_element_by_id('MainContent_btnByLettingYear')
        element.click()

        # Set 'Records Per Page' to 'All'
        element = self.browser.find_element_by_id('MainContent_drpPage')
        select = Select(element)
        select.select_by_value('20000')

        # Get links to download csvfrom HTML table
        element = self.browser.find_element_by_id('MainContent_gvabstractMenu')
        all_rows = element.find_elements_by_tag_name('a')

        # Extract contract IDs from the download links
        contract_ids = list()
        for row in all_rows:
            link = row.get_attribute('href')
            if 'ContractId' in link:
                contract_ids.append(int(link[-6:]))

        return contract_ids


    def __del__(self):
        self.browser.close()