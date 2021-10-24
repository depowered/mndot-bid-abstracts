from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.engine.create import create_engine
from sqlalchemy.ext.declarative import declarative_base


CONNECTION_STRING = "sqlite+pysqlite:///data/db.sqlite"


Base = declarative_base()

class Item(Base):
    __tablename__ = "Item"

    ItemID = Column(Integer, primary_key=True, unique=True)
    SpecCode = Column(Integer)
    UnitCode = Column(Integer)
    ItemCode = Column(Integer)
    Description = Column(String)
    Unit = Column(String)


class Abstract(Base):
    __tablename__ = "Abstract"

    AbstractID = Column(Integer, primary_key=True, unique=True)
    Year = Column(Integer)
    Processed = Column(String)


class Contract(Base):
    __tablename__ = "Contract"

    ContractID = Column(Integer, primary_key=True, unique=True)
    Year = Column(Integer)
    LetDate = Column(Date)
    SPNumber = Column(String)
    District = Column(String)
    County = Column(String)
    BidderID_0 = Column(Integer, ForeignKey("Bidder.BidderID"))
    BidderID_1 = Column(Integer, ForeignKey("Bidder.BidderID"))
    BidderID_2 = Column(Integer, ForeignKey("Bidder.BidderID"))


class Bid(Base):
    __tablename__ = "Bid"

    BidID = Column(Integer, primary_key=True, autoincrement=True)
    ContractID = Column(Integer, ForeignKey("Contract.ContractID"))
    ItemID = Column(Integer, ForeignKey("Item.ItemID"))
    Quantity = Column(Float)
    Engineer_UnitPrice = Column(Float)
    Engineer_TotalPrice = Column(Float)
    BidderID_0_UnitPrice = Column(Float)
    BidderID_0_TotalPrice = Column(Float)
    BidderID_1_UnitPrice = Column(Float)
    BidderID_1_TotalPrice = Column(Float)
    BidderID_2_UnitPrice = Column(Float)
    BidderID_2_TotalPrice = Column(Float)


class Bidder(Base):
    __tablename__ = "Bidder"

    BidderID = Column(Integer, primary_key=True, unique=True)
    Name = Column(String)


def main():
    # Creates blank database file
    eng = create_engine(CONNECTION_STRING)
    Base.metadata.create_all(eng)

if __name__ == '__main__':
    main()