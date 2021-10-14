# README

## Purpose
This project explores detailed bid data available through MnDOT's [Abstracts for Awarded Jobs](http://transport.dot.state.mn.us/PostLetting/Abstract.aspx) web application. This project scrapes, parses, aggregates, and analyzes data from the abstracts to provide insights on item cost trends and enable tooling for enhancing engineers estimates.

## Made possible by these open source projects
- selenium webdriver with Mozilla's geckodriver
- pandas
- requests

## Process
Structured project bid abstracts are available from MnDOT's web application that consists of the following three subtables:

1. Contract information including project location and letting date.

2. Bid items with total contract quantities, engineer's estimated cost, and each bidder's costs.

3. Unique bidder identification numbers for up to the three lowest bidders on the contract

The program starts by building a database table to both track the available contract abstracts and whether each abstract has been processed. This allows for the processing of newly bid projects without duplication of previously processed projects.

To process a bid abstract, the program makes a request for csv data from MnDOT's web application. The response data is stored in an 'Abstract' object that serves that data to the sub-table objects. The sub-table objects uses pandas to extract information from the abstract, rename and reorder columns, and insert the data into the database.

Finally, pandas is used to aggregate data queried from the database and cacluate weighted averages for each bid item. The resulting weighted averages can be exported to a csv for use as input into other tooling. The results are location filterable by MnDOT administrative district or county.

## Future Development
- Refactor the database management functionality from raw SQL statements to an Object Relational Mapper (like sqlalchemy). The goal of this change is to facilitate easier migration from SQLite to PostgreSQL to support distributed access to the processed data.
- Develop an interactive web application with functionality to explore, filter, and export aggregated insights.
- Explore container and/or packaging technologies to manage dependencies and improve deployability.