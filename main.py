import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
from utils import sql
from utils.custom import custom_logger
from utils.data_extractor import APIExtractor, CSVExtractor
from utils.custom import trainsaction_schema, bar_stock_schema
from utils.custom import get_cocktail_by_glass, extract_and_validate, generate_date_dim

# initialize the custom logger
logger = custom_logger(name="ETL Pipeline")

def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def create_db_connection(config):
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]
    db_name = config["database"]
    return create_engine(
        f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )


def load_to_stage(df, con, table_name):
    if not df.empty:
        logger.info("Loading data into %s", table_name)
        df.to_sql(name=table_name, con=con, if_exists="replace", index=False)
        logger.info("Loading data into %s completed", table_name)


def load_to_report(query, conn):
    logger.info("Loading data into %s", query)
    conn.execute(query)
    # conn.commit()
    logger.info("Loading data into %s completed", query)


def extract_transform(db_config, api_config, csv_config, conn):
    logger.info("Running load_data_to staging")

    # extract glass data from API
    glass_param = api_config["glass"]
    glass_table = db_config["glass_table_stage"]
    glass_df = extract_and_validate(parameters=glass_param, extract_func=APIExtractor)
    load_to_stage(glass_df, conn, glass_table)

    # cocktail data from API
    drink_param = api_config["cocktail"]
    drink_table = db_config["cocktail_table_stage"]
    glass_list = glass_df["glass"].unique().tolist()
    cocktail_df = get_cocktail_by_glass(
        parameters=drink_param, glass_list=glass_list, extract_func=APIExtractor
    )
    load_to_stage(cocktail_df, conn, drink_table)

    # bar stock data from csv
    bar_stock_param = csv_config["bar_stock"]
    stock_table = db_config["stock_table_stage"]
    stock_df = extract_and_validate(
        parameters=bar_stock_param, extract_func=CSVExtractor, schema=bar_stock_schema
    )
    load_to_stage(stock_df, conn, stock_table)

    # extract transaction data from csv validate and load into staging table
    transaction_table = db_config["transaction_table_stage"]
    transaction_df = pd.DataFrame()
    for param in csv_config["transactions"]:
        tmp = extract_and_validate(
            parameters=param, extract_func=CSVExtractor, schema=trainsaction_schema
        )
        tmp["location"] = param["name"]
        transaction_df = pd.concat([transaction_df, tmp], axis=0, ignore_index=True)
    load_to_stage(transaction_df, conn, transaction_table)

    # this will be a one time process as the date dimension table will be static
    if db_config["initial_load"]:
        date_table = db_config["date_table"]
        date_df = generate_date_dim(
            start_date="2020-01-01", end_date="2030-12-31", freq="H"
        )
        load_to_stage(date_df, conn, date_table)


def load(db_config, conn):
    logger.info("Updating report tables from staging")

    # update bars table
    stock_temp_table = db_config["stock_table_stage"]
    load_to_report(sql.insert_bar_table.format(temp_table=stock_temp_table), conn)

    # update glasses table
    glass_temp_table = db_config["glass_table_stage"]
    load_to_report(sql.insert_glass_table.format(temp_table=glass_temp_table), conn)

    # update cocktails table
    cocktail_temp_table = db_config["cocktail_table_stage"]
    load_to_report(
        sql.insert_cocktail_table.format(temp_table=cocktail_temp_table), conn
    )

    # update stock table
    stock_temp_table = db_config["stock_table_stage"]
    # load_to_report(sql.insert_stock_table.format(temp_table=stock_temp_table), conn)

    # update transaction table
    transaction_temp_table = db_config["transaction_table_stage"]
    load_to_report(
        sql.insert_transaction_table.format(temp_table=transaction_temp_table), conn
    )

    logger.info("Report tables update completed")


def main():
    logger.info("ETL process started")
    config = load_config("config.yaml")
    csv_config = config["CSV"]
    api_config = config["API"]
    db_config = config["DATABASE"]
    engine = create_db_connection(db_config)
    conn = engine.connect()
    extract_transform(db_config, api_config, csv_config, conn)
    load(db_config, conn)
    conn.close()
    logger.info("ETL process completed")


if __name__ == "__main__":
    main()
