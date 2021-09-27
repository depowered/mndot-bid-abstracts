from selenium.webdriver import Firefox
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from db import Cursor

'''
Scrapes MnDOT's abstract hosting page for all abstract download links
for a given year.
Appends a database with new entries from the web scrapping.
'''

# Abstract hosting webpage
URL = 'http://transport.dot.state.mn.us/PostLetting/Abstract.aspx'

# Set to False to launch a browser window
HEADLESS = True

#
# Prompt for year to scrape contract IDs
#
print("Enter desired year (YYYY): ")
year = input()

#
# Intialize browser
#
browser = None
if HEADLESS == True:
    opts = Options()
    opts.headless = True
    browser = Firefox(options=opts)
else:
    browser = Firefox()

browser.get(URL)

#
# Navigate to all results for the target year and extract Contract IDs
#
# Select letting year
element = browser.find_element_by_id('MainContent_drpLettingYear')
select = Select(element)
select.select_by_value(year)

# Submit initial form
element = browser.find_element_by_id('MainContent_btnByLettingYear')
element.click()

# Set 'Records Per Page' to 'All'
element = browser.find_element_by_id('MainContent_drpPage')
select = Select(element)
select.select_by_value('20000')

# Get links to download csvfrom HTML table
element = browser.find_element_by_id('MainContent_gvabstractMenu')
all_rows = element.find_elements_by_tag_name('a')

# Extract contract IDs from the download links
contract_ids = list()
for row in all_rows:
    link = row.get_attribute('href')
    if 'ContractId' in link:
        contract_ids.append(link[-6:])

#
# Add to database
#
with Cursor() as cur:
    int_year = int(year)
    processed = 0  # Initialize processed as 0 (False)
    for id in contract_ids:
        cur.execute('''INSERT OR IGNORE INTO Abstract
        ( AbstractID, Year, Processed ) VALUES ( ?, ?, ? )''',
        (id, int_year, processed))

browser.close()
print('Process complete.')
