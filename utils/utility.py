import copy
import logging
import pandas as pd
import pandera as pa
from typing import List, Dict, Callable, Optional



# Schema validation for transaction and bar data
bar_stock_schema = pa.DataFrameSchema(
    {
    "glass_type": pa.Column(str,nullable=False),
    "stock": pa.Column(int, checks=pa.Check.gt(-1), nullable=False), # check there is no negative stock
    "bar": pa.Column(str, nullable=False),
    },
)

trainsaction_schema = pa.DataFrameSchema(
    {
    "time": pa.Column("datetime64[ns]", nullable=False),
    "drink": pa.Column(str, nullable=False),
    "amount": pa.Column(float, checks=pa.Check.gt(0), nullable=False), # check all sales amount is greater than 0
    },
)

def custom_logger(level: str = "INFO", filename: str = "data_pipeline.log"):
    """
    This function returns a custom logger object.
    
    Returns
    -------
    logger: logging.Logger
        Logger object.
    """
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    logger.addHandler(stream_handler)
    # file_handler = logging.FileHandler(filename)
    # file_handler.setLevel(level)
    # file_handler.setFormatter(formatter)
    # logger.addHandler(file_handler)
    return logger


def generate_date_dim(start_date:str, end_date:str, freq:str ="H"):
    """ generate date attribute for the given range and returns it as a DataFrame.
    
    parameters
    ----------
    start_date : str
        Start date in "YYYY-MM-DD" format.
        
    end_date : str
        End date in "YYYY-MM-DD" format.
        
    freq : str
        Frequency of date intervals (default is "H" for hourly).
        
    Returns 
    -------
    df: pd.DataFrame
        DataFrame containing date-related columns.
    """
    df = pd.DataFrame({"date": pd.date_range(start=start_date, end=end_date,freq=freq)})
    df['date_id'] = df.date.dt.strftime('%Y-%m-%dT%H:%M:%S')
    df['calender_day'] = df.date.dt.day
    df['hour'] = df.date.dt.hour
    df['calender_week'] = df.date.dt.isocalendar().week
    df['calender_month'] = df.date.dt.strftime('%B')
    df['quarter'] = df.date.dt.quarter
    df['year'] = df.date.dt.year
    df['day_name'] = df.date.dt.strftime('%A')
    df['date'] = df.date.dt.date
    return df

def get_cocktail_by_glass(parameters: Dict, glass_list: List[str], extract_func: Callable) -> pd.DataFrame:
    """
    get list of cocktails based on the glass type from the API
    
    parameters
    ----------
    parameters : Dict
        This is the parameters for the APIExtractor class constructor.
        
    glass_list : List[str]
        List of glass types.
        
    extract_func : Callable
        This is the APIExtractor class.
        
    Returns
    -------
    df: pd.DataFrame
        DataFrame containing cocktails by glass type.
    """
    df = pd.DataFrame(columns=["glass", "drink"])
    for glass in glass_list:
        current_param = copy.deepcopy(parameters)
        current_param["request_obj"]["url"] = current_param["request_obj"]["url"].format(glass=glass)
        temp = extract_func(**current_param).fetch_data()
        temp["glass"] = glass
        df = pd.concat([df, temp], axis=0)
    return df


def extract_and_validate(parameters: Dict, extract_func: Callable, schema: Optional[pa.DataFrameSchema] = None) -> pd.DataFrame:
    """
    Extracts the data specification in the parameters Dict using the 
    extract_func and Validates the data with the schema.
    
    parameters
    ----------
    parameters : Dict
        This is the Parameters for the extract_func.

    extract_func : Callable
        This is the extract function which can be a class of  of (CSVExtractor| APIExtractor).
        
    schema : pa.DataFrameSchema
        This is the pandera DataFrameSchema object.
    
    Returns
    -------
    pd.DataFrame
        DataFrame containing extracted and validated data.
        
    """
    logger = custom_logger(level="INFO", filename="data_pipeline.log")
    try :
        df = extract_func(**parameters).fetch_data()
        if schema is None:
            return df
        validated_df = schema.validate(df, lazy=True)
        return validated_df
    except pa.errors.SchemaErrors as e:
        error_df = e.failure_cases
        errors = list(zip(error_df.column, error_df.check))
        logger.error(f"Schema validation failed for columns {errors} in {parameters['name']} file", exc_info=True)
        raise e