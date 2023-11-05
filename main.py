import os
import yaml
import hashlib
import pandas as pd
from sqlalchemy import create_engine
from utils.utility import get_drinks_by_glass, extract_and_validate, generate_date_dim
from utils.utility import trainsaction_schema, bar_schema
from utils.data_extractor import APIExtractor, CSVExtractor

pk_generator = lambda x : hashlib.shake_256(str(x).encode()).hexdigest(10)

# Load config file
config_path = "config.yaml"
config = yaml.safe_load(open("config.yaml"))

db_config = config["DATABASE"]
csv_config = config["CSV"]
api_config = config["API"]

# get database configurations
db_user = os.environ["DB_USER"]
db_password = os.environ["DB_PASSWORD"]
db_host = os.environ["DB_HOST"]
db_port = os.environ["DB_PORT"]
db_name = os.environ["DB_NAME"]


# create database engine connection
engine = create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# extract glass data from API
glass_param = api_config["glass"]
glass_table = db_config["glass_table"]
glass_df = extract_and_validate(parameters=glass_param, extract_func=APIExtractor)
glass_df["glass_id"] = glass_df["glass"].apply(pk_generator)
glass_df.to_sql(name=glass_table, con=engine, if_exists="replace", index=False)

# extract drink data from API and validate
drink_param = api_config["drink"]
drink_table = db_config["drink_table"]
glass_list = glass_df["glass"].unique().tolist()
drink_df = get_drinks_by_glass(parameters=drink_param, glass_list=glass_list, extract_func=APIExtractor)
drink_df.to_sql(name=drink_table, con=engine, if_exists="replace", index=False)

# extract bar stock data from csv and validate
bar_param = csv_config["bar"]
bar_table = db_config["bar_table"]
stock_table = db_config["stock_table"]
stock_df = extract_and_validate(parameters=bar_param, extract_func=CSVExtractor, schema=bar_schema)
bar_df = stock_df["bar"].apply(pk_generator)

#save data into staging table
stock_df.to_sql(name=stock_table, con=engine, if_exists="replace", index=False)
bar_df.to_sql(name=bar_table, con=engine, if_exists="replace", index=False)


# extract transaction data from csv and validate transaction data from csvfile
transaction_table = db_config["transaction_table"]
transaction_df = pd.DataFrame()
for param in csv_config["transactions"]:
    tmp = extract_and_validate(parameters=param, extract_func=CSVExtractor, schema=trainsaction_schema)
    tmp["location"] = param["name"]
    transaction_df = pd.concat([transaction_df, tmp], axis=0, ignore_index=True)

# generate date dimension table with a grain of 1 hour
# this will be a one time process as the date dimension table will be static
date_table = db_config["date_table"]

#save data into staging table
date_df = generate_date_dim(start_date="2020-01-01", end_date="2020-12-31", freq="H")




# bar_table = db_config["bar_table"]
# dim_bar = bar_df[["bar"]].drop_duplicates()
# dim_bar["bar_id"] = dim_bar["bar"].apply(pk_generator)
# dim_bar.to_sql(name=bar_table, con=engine, if_exists="append", index=False)
