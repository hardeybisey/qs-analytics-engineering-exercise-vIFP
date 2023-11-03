# recomendations

1) We talk to the data team if it they can be consistent with the format 
2) we build our data pipeline with the format of our input data


# data issue found during validation
1) Dates are in different format in different stores
2) One of the rows in the bar data has incorrect entry, this might be caused during data entry


# Example parameters for CSV and API extraction
csv_params = {
    "pandas_kwargs": {
        "filepath_or_buffer": "data.csv",
        "sep": ",",
        "index_col": None,
        "parse_dates": None,
        "dtype": None
    },
    "columns_mapping": {
    }
    drop_columns : ,

}

api_params = {
    "request_obj": {
        "url": "https://api.example.com/data", #required
        "params": {"param1": "value1"}, #optional
        "headers": {"Authorization": "Bearer token"}, #optional
        "timeout": 10 #optional
    },
    "data_key": "data", #required
    "columns_mapping": {
        "api_column_name1": "new_column_name1",
        "api_column_name2": "new_column_name2"
    } #optional
}

csv_params = {
    "pandas_kwargs": {
        "filepath_or_buffer": str,
        "sep": str, 
        "index_col": Optional[str] , 
        "parse_dates": Optional[List[str]],
        "dtype": Optional[Dict[str,str]]
        },
    "columns_mapping": Optional[Dict[str,str]],
}

api_params = {
    "rquest_obj": {
        "url": str,
        "params": Optional[Dict[str,str]],
        "headers": Optional,
        "auth": Optional,
        "timeout": Optional,
    },
    "data_obj": str,
    "columns_mapping": Dict[str,str],
}