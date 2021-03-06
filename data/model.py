from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.engine.create import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


CONNECTION_STRING = "sqlite+pysqlite:///data/db.sqlite"


engine = create_engine(CONNECTION_STRING)

Session = sessionmaker(engine)


Base = declarative_base()


class Item2018(Base):
    __tablename__ = "Item2018"

    ItemID_2018 = Column(Integer, primary_key=True, unique=True)
    SpecCode_2018 = Column(String)
    UnitCode_2018 = Column(String)
    ItemCode_2018 = Column(String)
    Description_2018 = Column(String)
    Unit_2018 = Column(String)


    def __str__(self) -> str:
        return f'Item(Description={self.Description}, Unit={self.Unit})'


    def __repr__(self) -> str:
        a = f'ItemID = {self.ItemID_2018}'
        b = f'SpecCode = {self.SpecCode_2018}'
        c = f'UnitCode = {self.UnitCode_2018}'
        d = f'ItemCode = {self.ItemCode_2018}'
        e = f'Description = {self.Description_2018}'
        f = f'Unit = {self.Unit_2018}'
        return ', '.join( [a, b, c, d, e, f] )


class Item2020(Base):
    __tablename__ = "Item2020"

    ItemID_2020 = Column(Integer, primary_key=True, unique=True)
    SpecCode_2020 = Column(String)
    UnitCode_2020 = Column(String)
    ItemCode_2020 = Column(String)
    Description_2020 = Column(String)
    Unit_2020 = Column(String)
    Item2018_ID = Column(Integer)


    def __str__(self) -> str:
        return f'Item(Description={self.Description}, Unit={self.Unit})'


    def __repr__(self) -> str:
        a = f'ItemID = {self.ItemID_2020}'
        b = f'SpecCode = {self.SpecCode_2020}'
        c = f'UnitCode = {self.UnitCode_2020}'
        d = f'ItemCode = {self.ItemCode_2020}'
        e = f'Description = {self.Description_2020}'
        f = f'Unit = {self.Unit_2020}'
        return ', '.join( [a, b, c, d, e, f] )


class Abstract(Base):
    __tablename__ = "Abstract"

    AbstractID = Column(Integer, primary_key=True, unique=True)
    Year = Column(Integer)
    Processed = Column(String)


    def __str__(self) -> str:
        return f'Abstract(AbstractID={self.AbstractID})'


    def __repr__(self) -> str:
        return f'AbstractID = {self.AbstractID}, Year = {self.Year}, Processed = {self.Processed}'


class Contract(Base):
    __tablename__ = "Contract"

    ContractID = Column(Integer, primary_key=True, unique=True)
    Year = Column(Integer)
    LetDate = Column(String)
    SPNumber = Column(String)
    District = Column(String)
    County = Column(String)
    BidderID_0 = Column(Integer, ForeignKey("Bidder.BidderID"))
    BidderID_1 = Column(Integer, ForeignKey("Bidder.BidderID"))
    BidderID_2 = Column(Integer, ForeignKey("Bidder.BidderID"))


    def __str__(self) -> str:
        return f'Contract(ContractID={self.ContractID})'


    def __repr__(self) -> str:
        a = f'ContractID = {self.ContractID}'
        b = f'Year = {self.Year}'
        c = f'LetDate = {self.LetDate}'
        d = f'SPNumber = {self.SPNumber}'
        e = f'District = {self.District}'
        f = f'County = {self.County}'
        g = f'BidderID_0 = {self.BidderID_0}'
        h = f'BidderID_1 = {self.BidderID_1}'
        i = f'BidderID_2 = {self.BidderID_2}'

        return ', '.join( [a, b, c, d, e, f, g, h, i] )


class Bid(Base):
    __tablename__ = "Bid"

    BidID = Column(Integer, primary_key=True, unique=True)
    ContractID = Column(Integer, ForeignKey("Contract.ContractID"))
    ItemID = Column(Integer)
    SpecYear = Column(Integer)
    Quantity = Column(Float)
    Engineer_UnitPrice = Column(Float)
    Engineer_TotalPrice = Column(Float)
    BidderID_0_UnitPrice = Column(Float)
    BidderID_0_TotalPrice = Column(Float)
    BidderID_1_UnitPrice = Column(Float)
    BidderID_1_TotalPrice = Column(Float)
    BidderID_2_UnitPrice = Column(Float)
    BidderID_2_TotalPrice = Column(Float)


    def __str__(self) -> str:
        return f'Bid(BidID={self.BidID})'


    def __repr__(self) -> str:
        a = f'BidID = {self.BidID}'
        b = f'ContractID = {self.ContractID}'
        c = f'ItemID = {self.ContractID}'
        d = f'Quantity = {self.Quantity}'
        e = f'Engineer_UnitPrice = {self.Engineer_UnitPrice}'
        f = f'Engineer_TotalPrice = {self.Engineer_TotalPrice}'
        g = f'BidderID_0_UnitPrice = {self.BidderID_0_UnitPrice}'
        h = f'BidderID_0_TotalPrice = {self.BidderID_0_TotalPrice}'
        i = f'BidderID_1_UnitPrice = {self.BidderID_1_UnitPrice}'
        j = f'BidderID_1_TotalPrice = {self.BidderID_1_TotalPrice}'
        k = f'BidderID_2_UnitPrice = {self.BidderID_2_UnitPrice}'
        l = f'BidderID_2_TotalPrice = {self.BidderID_2_TotalPrice}'

        return ', '.join( [a, b, c, d, e, f, g, h, i, j, k, l] )


class Bidder(Base):
    __tablename__ = "Bidder"

    BidderID = Column(Integer, primary_key=True, unique=True)
    Name = Column(String)


    def __str__(self) -> str:
        return f'Bidder(Name={self.Name})'


    def __repr__(self) -> str:
        return f'BidderID = {self.BidderID}, Name = {self.Name}'


def main():
    # Creates blank database file
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    main()