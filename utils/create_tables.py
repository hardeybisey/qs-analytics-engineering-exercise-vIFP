import os 
from sqlalchemy import (
    create_engine, 
    Table, 
    Column, 
    Integer, 
    String, 
    MetaData, 
    DateTime, 
    Float
    )

from sqlalchemy import UniqueConstraint, ForeignKey

metadata_obj = MetaData()

cocktail_table = Table(
    "cocktail",
    metadata_obj,
    Column("cocktail_id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    Column("glass", String, nullable=False),
    UniqueConstraint("name", "glass"),
)

glass_table = Table(
    "glass",
    metadata_obj,
    Column("glass_id", Integer, primary_key=True),
    Column("glass", String, nullable=False),
    UniqueConstraint("glass"),
)

stock_table = Table(
    "stock_bar",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("glass_id", Integer, ForeignKey("glass.glass_id")),
    Column("bar_id", Integer, ForeignKey("dim_bar.bar_id")),
    UniqueConstraint("glass_id", "bar_id"),
)

fact_transaction_table = Table(
    "fact_transaction",
    metadata_obj,
    Column("transaction_id", Integer, primary_key=True),
    Column("date_id", String, ForeignKey("dim_date.date_id")),
    Column("bar_id", Integer, ForeignKey("dim_bar.bar_id")),
    Column("cocktail_id", Integer, ForeignKey("cocktail.cocktail_id")),
    Column("amount", Float, nullable=False),
)

dim_bar_table = Table(
    "dim_bar",
    metadata_obj,
    Column("bar_id", Integer, primary_key=True),
    Column("name", String, nullable=False),
    UniqueConstraint("name"),
)

dim_date_table = Table(
    "dim_date",
    metadata_obj,
    Column("date_id", String, primary_key=True),
    Column("date", DateTime, nullable=False),
    Column("day_name", String, nullable=False),
    Column("calender_day", Integer, nullable=False),
    Column("hour", Integer, nullable=False),
    Column("calender_week", Integer, nullable=False),
    Column("calender_month", String, nullable=False),
    Column("quarter", Integer, nullable=False),
    Column("year", Integer, nullable=False),
)

if __name__ == "__main__":
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]
    db_name = "test2" #os.environ["DB_NAME"]
    
    engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
    metadata_obj.create_all(engine, checkfirst=True)
    print("Tables created successfully")