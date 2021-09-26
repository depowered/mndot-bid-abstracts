# README #

## Purpose ##
This project explores detailed bid data available through MnDOT's [Abstracts for Awarded Jobs](http://transport.dot.state.mn.us/PostLetting/Abstract.aspx) web application. This project scrapes, parses, aggregates, and analyzes data from the abstracts to provide insights on item cost trends and enable tooling for enhancing engineers estimates.

## Primary Dependencies ##
1. selenium webdriver with Mozilla's geckodriver
2. pandas
3. requests

## Process ##
The MnDOT bid abstracts are delivered in a structured csv that consists of three subtables:
1. Contract information including project location and letting date.
2. Bid items with total contract quantities, engineer's estimated cost, and each bidder's costs.
3. Unique bidder identification numbers for up to the three lowest bidders on the contract

The program starts by building a database table to both track the available contract ids and whether they have been processesd. This allows for the addition of new abstracts with duplicating the download and processing of abstracts already in the database.

When an abstract is processes, the program makes a HTTP request for csv data. The response data is stored in a custom Abstract object that serves that data to the downstream processing objects. The processing objects use pandas dataframes to extract each subtable from the csv data, rename and reorder columns, and insert the data into the SQLite database tables.

Finally, pandas is used to extract insights from the aggregated bid data. Currently only calculation of weighted average unit price each item is developed. The resulting weighted averages are output to a csv for use as input into other tooling. The results are location filterable by MnDOT administrative district or county.

## Future Development ##
- Migrate from SQLite to PostgreSQL to support distributed access to the processed data
- Refactor web scraping from selenium to a requests-beautifulsoup-combo to eliminate the need to install a browser
- Develop an interactive web application with functionality to explore, filter, and export aggregated insights
- Package the program using a container technology for managing the environment and improving deployability