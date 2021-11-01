import build.db as db
import build.scraper as scraper
import build.processor as processor
import weightedavg

'''Builds a populated database from scratch.'''

# Build the empty database
db.create_all_tables()

# Populate item table
item_csv = 'data/2018_TrnsportItemList.csv'
db.csv_to_item_table(item_csv)

# Scrape abstracts for years 2018-2021
years = ['2021', '2020', '2019', '2018']
for year in years:
    scraper.main(year)

# Process all scraped abstracts
processor.main()

# Compute weighted average and output to csv
weightedavg.main()