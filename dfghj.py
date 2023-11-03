import os 
import logging
from sqlalchemy import (
    create_engine, 
    Table, 
    Column, 
    Integer, 
    String, 
    MetaData, 
    ForeignKey, 
    DateTime, 
    Float
    )



metadata_obj = MetaData()

transaction_shema = Table(
    "FactTransactions",
    metadata_obj,
    Column(name="transaction_id", type=Integer, primary_key=True),
    Column(name="bar_id", type=Integer, nullable=False),
    Column(name="amount", type=Float, nullable=False),
    Column(name="date", type=DateTime, nullable=False),
)

glass = Table(
    "DimGlass",
    metadata_obj,
    Column(name="glass_id", type=Integer, primary_key=True),
    Column(name="name", type=String(16), nullable=False),
)

bar = Table(
    "DimBar",
    metadata_obj,
    Column(name="bar_id", type=Integer, primary_key=True),
    Column(name="name", type=String(16), nullable=False),
)

cocktail = Table(
    "DimCockTail",
    metadata_obj,
    Column(name="cocktail_id", type=Integer, primary_key=True),
    Column(ForeignKey("DimGlass.glass_id"), name="glass_id", type=Integer, nullable=False),
    Column(name="name", type=String(16), nullable=False),
)

if __name__ == "__main__":
    PG_USER = os.environ.get('PG_USER', 'postgres')
    PG_PASS = os.environ.get('PG_PASS', 'postgres')
    PG_HOST = os.environ.get('PG_HOST', 'localhost')
    PG_PORT = os.environ.get('PG_PORT', '5432')
    PG_DB = os.environ.get('PG_DB', 'quantspark')
    
    engine = create_engine(f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}")
    metadata_obj.create_all(engine, checkfirst=True)