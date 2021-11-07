from sqlalchemy import select
from build.scraper import AbstractIDScraper
from data.model import Abstract, Session
from datetime import datetime


current_year = datetime.now().year


def get_abstract_ids_from_db(session: Session):
    '''Retrieves a list of all Abstract IDs from the Abstract database table.'''
    statement = select(Abstract.AbstractID)
    result = session.execute(statement)

    db_abstract_ids = []
    for id in result.scalars():
        db_abstract_ids.append(id)

    return db_abstract_ids


def get_new_abstract_objs(scraper: AbstractIDScraper, db_abstract_ids: list):
    '''Generates list of new Abstract table objects to be inserted into the database.'''
    abstract_objs = []

    for scrapped_id in scraper.abstract_ids:
        if scrapped_id not in db_abstract_ids:
            ab_obj = Abstract(
                    AbstractID=scrapped_id,
                    Year=scraper.year,
                    Processed='NO'
                )
            abstract_objs.append(ab_obj)

    return abstract_objs


def scrape_abstract_ids(year: int = current_year):
    '''Scrape abstract IDs for given year and insert into database.'''
    with Session() as session:
        # get a list of all Abstract IDs already in the database
        db_abstract_ids = get_abstract_ids_from_db(session)

        scraper = AbstractIDScraper(year=year)

        # compare scraped ids to list from the database
        new_abstract_objs = get_new_abstract_objs(scraper, db_abstract_ids)

        # Insert any new abstracts into the database
        for obj in new_abstract_objs:
            session.add(obj)

        session.commit()

    print("Scraping complete.")
    print(f'Inserted {len(new_abstract_objs)} new Abstract IDs into the database.')


def main():
    # scrape new ids for current year
    scrape_abstract_ids()


if __name__ == '__main__':
    main()