DATA_DIR = 'data/'

glasses_params = {
    "data": "glasses_endpoint",
    "request_obj": {
        "url": "https://www.thecocktaildb.com/api/json/v1/1/list.php?g=list",
        },
    "data_field": "drinks",
    "columns_mapping": {"strGlass": "glass"}
}

drink_params = {
    "data": "drinks_endpoint",
    "request_obj": {
        "url": "https://www.thecocktaildb.com/api/json/v1/1/filter.php?g={glass}",
        },
    "data_field": "drinks",
    "columns_mapping": {"strDrink": "drink"},
    "drop_columns": ["strDrinkThumb", "idDrink"]
    }

bar_params = {
    "data": "bar_data_csv",
    "pandas_kwargs": {
        "filepath_or_buffer": DATA_DIR + 'bar_data.csv',
        }
}

budapest_transaction_params = {
    "data": "budapest_transaction_csv",
    "pandas_kwargs": {
        "filepath_or_buffer": DATA_DIR + 'budapest.csv',
        "parse_dates": [1],
        "date_format": '%Y-%m-%d %H:%M:%S',
        "index_col": [0],
        },
    "columns_mapping": {"TS": "time", "ital": "drink", "költség": "amount"},
    }
    
london_transaction_params = {
    "data": "london_transaction_csv",
    "pandas_kwargs": {
        "filepath_or_buffer": DATA_DIR + 'london_transactions.csv',
        "sep": '\t', 
        "header": None,
        "parse_dates": [1],
        "date_format": '%Y-%m-%d %H:%M:%S'
        },
    "columns_mapping": {1: "time", 2: "drink", 3: "amount"}, 
    "drop_columns": [0]
    }

ny_transaction_params = {
    "data": "ny_transaction_csv",
    "pandas_kwargs": {
        "filepath_or_buffer": DATA_DIR + 'ny.csv',
        "parse_dates": [1], 
        "date_format": '%m-%d-%Y %H:%M',
        "index_col": [0],
        },
    }