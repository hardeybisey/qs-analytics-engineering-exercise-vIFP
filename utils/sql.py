import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, UniqueConstraint, ForeignKey
from sqlalchemy import Column, Integer, String, DateTime, DECIMAL
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Bar(Base):
    __tablename__ = "bars"
    id = Column(Integer, primary_key=True)
    bar = Column(String, nullable=False)
    UniqueConstraint("bar")


class Glass(Base):
    __tablename__ = "glasses"
    id = Column(Integer, primary_key=True)
    glass = Column(String, nullable=False)
    UniqueConstraint("glass")


class Cocktail(Base):
    __tablename__ = "cocktails"
    id = Column(Integer, primary_key=True)
    glass_id = Column(Integer, ForeignKey("glasses.id"), nullable=False)
    drink = Column(String, nullable=False)
    UniqueConstraint("drink")


class BarStock(Base):
    __tablename__ = "bar_stock"
    id = Column(Integer, primary_key=True)
    bar_id = Column(Integer, ForeignKey("bars.id"), nullable=False)
    glass_id = Column(Integer, ForeignKey("glasses.id"), nullable=False)
    stock = Column(Integer)


class Transaction(Base):
    __tablename__ = "fact_transactions"
    id = Column(Integer, primary_key=True)
    bar_id = Column(Integer, ForeignKey("bars.id"), nullable=False)
    cocktail_id = Column(Integer, ForeignKey("cocktails.id"), nullable=False)
    date = Column(DateTime, nullable=False)
    amount = Column(DECIMAL(2), nullable=False)


class Date(Base):
    __tablename__ = "dim_date"
    date_id = Column(String, primary_key=True, nullable=False)
    date = Column(DateTime, nullable=False)
    day_name = Column(String, nullable=False)
    calendar_day = Column(Integer, nullable=False)
    week_number = Column(Integer, nullable=False)
    hour = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)
    quarter = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    hour = Column(Integer, nullable=False)


insert_bar_table = """
MERGE INTO bars T
USING 
    (SELECT DISTINCT bar 
    FROM {temp_table}) S
ON T.bar = S.bar
WHEN NOT MATCHED THEN
	INSERT (bar) 
    VALUES (S.bar);
"""

insert_glass_table = """
MERGE INTO glasses T
USING {temp_table} S
ON T.glass = S.glass
WHEN NOT MATCHED THEN 
	INSERT (glass) 
    VALUES (S.glass)
"""

insert_cocktail_table = """
MERGE INTO cocktails T
USING 
    (SELECT g.id AS glass_id , c.drink 
    FROM {temp_table} c
    LEFT JOIN glasses g
    ON c.glass=g.glass
    ) S
ON T.glass_id = S.glass_id AND T.drink = S.drink
WHEN NOT MATCHED THEN 
    INSERT (glass_id, drink) 
    VALUES (S.glass_id, S.drink)
"""

insert_stock_table = """
MERGE INTO bar_stock T
USING 
    (SELECT b.id AS bar_id, g.id AS glass_id, stock 
    FROM {temp_table} ts
    LEFT JOIN bars b
    ON ts.bar=b.bar
    LEFT JOIN glasses g
    ON ts.glass_type=g.glass
    ) S
ON T.bar_id = S.bar_id AND T.glass_id = S.glass_id
WHEN NOT MATCHED THEN 
    INSERT (bar_id, glass_id, stock) 
    VALUES (S.bar_id, S.glass_id, S.stock)
"""

insert_transaction_table = """
MERGE INTO fact_transactions H
USING (
    SELECT b.id AS bar_id, c.id AS cocktail_id, amount, tx.time
    FROM {temp_table} tx
    INNER JOIN bars b ON tx.location=b.bar
    INNER JOIN cocktails c ON tx.drink=c.drink
    ) S
ON H.bar_id = S.bar_id AND H.cocktail_id = S.cocktail_id AND H.date = S.time
WHEN NOT MATCHED THEN 
    INSERT (bar_id, cocktail_id, amount, date) 
    VALUES (S.bar_id, S.cocktail_id, S.amount, S.time)
"""

if __name__ == "__main__":
    db_user = os.environ["PG_USER"]
    db_password = os.environ["PG_PASSWORD"]
    db_host = os.environ["PG_HOST"]
    db_port = os.environ["PG_PORT"]
    db_name = os.environ["PG_DATABASE"]

    print("Creating tables")
    engine = create_engine(
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    Base.metadata.create_all(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.commit()
    session.close()
    print("Tables created")
