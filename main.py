import os
import yaml
import pandas as pd
from sqlalchemy import create_engine
from utils import sql
from utils.custom import custom_logger
from utils.data_extractor import APIExtractor, CSVExtractor
from utils.custom import trainsaction_schema, bar_stock_schema
from utils.custom import get_cocktail_by_glass, extract_and_validate, generate_date_dim

CONFIG_PATH = "config.yaml"


def load_config(config_path):
    """Load configuration from a YAML file."""
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def create_db_connection():
    """Create a database connection based on configuration."""
    db_user = os.environ["PG_USER"]
    db_password = os.environ["PG_PASSWORD"]
    db_host = os.environ["PG_HOST"]
    db_port = os.environ["PG_PORT"]
    db_name = os.environ["PG_DATABASE"]
    return create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")


def load_to_stage(dataframe, connection, table_name):
    """Load a DataFrame into a database staging table."""
    if not dataframe.empty:
        logger.info("Loading data into %s", table_name)
        dataframe.to_sql(name=table_name, con=connection, if_exists="replace", index=False)
        logger.info("Loading data into %s completed", table_name)


def load_to_report(query, connection):
    """Execute an SQL query to load data into a report table."""
    logger.info("Loading data into %s", query)
    connection.execute(query)
    connection.execute("COMMIT")
    logger.info("Loading data into %s completed", query)


def extract_transform_and_load(db_config, api_config, csv_config, connection):
    """Extract data from various sources, transform, and load into staging tables."""

    logger.info("Running load_data_to staging")

    # glass data from API
    glass_param = api_config["glass"]
    glass_table = db_config["glass_table_stage"]
    glass_df = extract_and_validate(parameters=glass_param, extract_func=APIExtractor)
    load_to_stage(glass_df, connection, glass_table)

    # cocktail data from API
    drink_param = api_config["cocktail"]
    drink_table = db_config["cocktail_table_stage"]
    glass_list = glass_df["glass"].unique().tolist()
    cocktail_df = get_cocktail_by_glass(parameters=drink_param, glass_list=glass_list, extract_func=APIExtractor)
    load_to_stage(cocktail_df, connection, drink_table)

    # bar stock data from csv
    bar_stock_param = csv_config["bar_stock"]
    stock_table = db_config["stock_table_stage"]
    stock_df = extract_and_validate(parameters=bar_stock_param, extract_func=CSVExtractor, schema=bar_stock_schema)
    load_to_stage(stock_df, connection, stock_table)

    # extract transaction data from csv validate and load into staging table
    transaction_table = db_config["transaction_table_stage"]
    transaction_df = pd.DataFrame()
    for param in csv_config["transactions"]:
        tmp = extract_and_validate(parameters=param, extract_func=CSVExtractor, schema=trainsaction_schema)
        tmp["location"] = param["name"]
        transaction_df = pd.concat([transaction_df, tmp], axis=0, ignore_index=True)
    load_to_stage(transaction_df, connection, transaction_table)

    # this will be a one time process as the date dimension table will be static
    if db_config["initial_load"]:
        date_table = db_config["date_table"]
        date_df = generate_date_dim(start_date="2020-01-01", end_date="2030-12-31", freq="H")
        load_to_stage(date_df, connection, date_table)


def update_report_tables(db_config, connection):
    """Update report tables from staging tables."""

    logger.info("Updating report tables from staging")

    # update bars table
    stock_temp_table = db_config["stock_table_stage"]
    load_to_report(sql.insert_bar_table.format(temp_table=stock_temp_table), connection)

    # update glasses table
    glass_temp_table = db_config["glass_table_stage"]
    load_to_report(sql.insert_glass_table.format(temp_table=glass_temp_table), connection)

    # update cocktails table
    cocktail_temp_table = db_config["cocktail_table_stage"]
    load_to_report(sql.insert_cocktail_table.format(temp_table=cocktail_temp_table), connection)

    # update stock table
    stock_temp_table = db_config["stock_table_stage"]
    load_to_report(sql.insert_stock_table.format(temp_table=stock_temp_table), connection)

    # update transaction table
    transaction_temp_table = db_config["transaction_table_stage"]
    load_to_report(sql.insert_transaction_table.format(temp_table=transaction_temp_table), connection)

    logger.info("Report tables update completed")


def main():
    logger.info("ETL process started")
    config = load_config(CONFIG_PATH)
    csv_config = config["CSV"]
    api_config = config["API"]
    db_config = config["DATABASE"]
    engine = create_db_connection()
    connection = engine.connect()

    try:
        extract_transform_and_load(db_config, api_config, csv_config, connection)
        update_report_tables(db_config, connection)
        logger.info("ETL process completed")
    except Exception as e:
        logger.error("ETL process encountered an error: %s", str(e))
    finally:
        connection.close()


if __name__ == "__main__":
    logger = custom_logger(name="ETL Pipeline")  # Initialize the custom logger.
    main()