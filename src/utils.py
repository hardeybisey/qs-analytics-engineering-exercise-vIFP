import requests
import logging
import pandas as pd
import pandera as pa
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import List, Dict, Optional, Any as AnyType

logger = logging.getLogger(__name__)


bar_schema = pa.DataFrameSchema(
    {
    "glass_type": pa.Column(str,nullable=False),
    "stock": pa.Column(int, checks=pa.Check.gt(0), nullable=False), # check there is no negative stock
    "bar": pa.Column(str, nullable=False),
    },
)

trainsaction_schema = pa.DataFrameSchema(
    {
    "time": pa.Column("datetime64[ns]", nullable=False),
    "drink": pa.Column(str, nullable=False),
    "amount": pa.Column(float, checks=pa.Check.gt(0), nullable=False), # check all sales amount is positive
    },
)

class BaseExtractor(ABC):
    """
    This class defines the common  interface for all data extraction methods.
    """
    @abstractmethod
    def fetch_data(self, **kwargs):
        pass
    
class CSVExtractor(BaseExtractor):
    """
    Extract data from a CSV file with the specified parameters.
    
    parameters
    ----------
    data : str
        This is the description for the source of the data.
        
    pandas_kwargs : Dict[AnyType,AnyType]
        This is the parameters for the pandas.read_csv function.
        
    columns_mapping : Optiona[Dict[AnyType,AnyType]]
        This is the parameters for the pandas.DataFrame.rename function.
        
    drop_columns : Optiona[List[AnyType]] 
        This is the parameters for the pandas.DataFrame.drop function.
    """
    def __init__(
        self, 
        data: str,
        pandas_kwargs: Dict[AnyType,AnyType], 
        columns_mapping: Optional[Dict[AnyType,AnyType]]=None, 
        drop_columns: Optional[List[AnyType]]=None
        ):
        self.pandas_kwargs = pandas_kwargs
        self.columns_mapping = columns_mapping
        self.drop_columns = drop_columns

    def fetch_data(self) -> Optional[pd.DataFrame]:
        """
        This method reads data from a CSV file using the configurations passed to the class constructor 
        and returns it as a DataFrame.
        
        Returns
        -------
        df: pd.DataFrame
            DataFrame containing extracted data.
        
        Raises
        ------
        Exception: If an error occured while reading the file and the error message is logged.
        """
        try:
            df = pd.read_csv(**self.pandas_kwargs)
            if self.drop_columns:
                df.drop(columns=self.drop_columns, inplace=True)
            if self.columns_mapping:
                df.rename(columns=self.columns_mapping, inplace=True)
            return df
        except Exception as e:
            logger.error(f"The file specified in the configuration {self.pandas_kwargs} does not exist", exc_info=True)

class APIExtractor(BaseExtractor):
    
    """
    Extract data from a Web API with the specified parameters.
    
    parameters
    ----------
    data : str
        This is the description for the source of the data.
        
    request_obj : Dict[AnyType,AnyType]
        This is the parameters for the requests.get function.
        
    data_field : str
        This is the key for the data field in the API response.

    columns_mapping : Optiona[Dict[AnyType,AnyType]]
        This is the parameters for the pandas.DataFrame.rename function.
        
    drop_columns : Optiona[List[AnyType]]
        This is the parameters for the pandas.DataFrame.drop function.
    """
    
    def __init__(
        self, 
        data: str,
        request_obj: Dict[AnyType,AnyType], 
        data_field: str, 
        columns_mapping: Optional[Dict[AnyType,AnyType]]=None,
        drop_columns: Optional[List[AnyType]]=None
        ):
        self.data = data
        self.request_obj = request_obj
        self.data_field = data_field
        self.columns_mapping = columns_mapping
        self.drop_columns = drop_columns
        
    def fetch_data(self) -> Optional[pd.DataFrame]:
        """
        This method reads data from an API using the configurations passed to the class constructor 
        and returns it as a DataFrame.
        
        Returns
        -------
        df: pd.DataFrame
            DataFrame containing extracted data.
        
        Raises
        ------
        Exception: If an error occured while reading data from the API and the error message is logged.
        """
        try:
            resp = requests.get(**self.request_obj)
            resp = resp.json()[self.data_field]   
            df = pd.DataFrame(resp)
            if self.drop_columns:
                df.drop(columns=self.drop_columns, inplace=True)
            if self.columns_mapping:
                df.rename(columns=self.columns_mapping, inplace=True)  
            return df
        except Exception as e:
            logger.error(f"parsing API with configuration {self.request_obj} failed", exc_info=True)
            
def generate_date_dim(start:str, end:str, freq:str ="H"):
    """ generate date attribute for the given range and returns it as a DataFrame.
    
    parameters
    ----------
    start : str
        Start date in "YYYY-MM-DD" format.
        
    end : str
        End date in "YYYY-MM-DD" format.
        
    freq : str
        Frequency of date intervals (default is "H" for hourly).
        
    Returns 
    -------
    df: pd.DataFrame
        DataFrame containing date-related columns.
    """
    df = pd.DataFrame({"date": pd.date_range(start=start, end=end,freq=freq)})
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

def get_drinks_by_glass(parameters: Dict, glass_list: List[str], extract_func: Callable) -> pd.DataFrame:
    """
    get list of drinks based on the glass type from the API
    
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
        DataFrame containing drinks by glass type.
    """
    df = pd.DataFrame(columns=["glass", "drink"])
    for glass in glass_list:
        parameters["request_obj"]["url"] = parameters["request_obj"]["url"].format(glass=glass)
        temp = extract_func(**parameters).fetch_data()
        temp["glass"] = glass
        df = pd.concat([df, temp], axis=0)
    return df


def extract_validate(extract_func: Callable, schema: pa.DataFrameSchema, parameters: Dict, ) -> pd.DataFrame:
    """
    This function extracts and validate the data specification in the parameters Dict using the extract_func and schema.
    
    parameters
    ----------
    extract_func : Callable
        This is the extract function which can be a class of  of (CSVExtractor| APIExtractor).
        
    parameters : Dict
        This is the Parameters for the extract_func
        
    schema : pa.DataFrameSchema
        This is the pandera DataFrameSchema object.
    
    Returns
    -------
    pd.DataFrame
        DataFrame containing extracted and validated data.
        
    Raises
    ------
    pa.errors.SchemaErrors
        If schema validation fails, an error message is logged, and the exception is raised.
    """

    try :
        df = extract_func(**parameters).fetch_data()
        validated_df = schema.validate(df, lazy=True)
        return validated_df
    except pa.errors.SchemaErrors as e:
        error_df = e.failure_cases
        errors = list(zip(error_df.column, error_df.check))
        logger.error(f"Schema validation failed for columns {errors} in {parameters['data']} file")
        raise Exception(f"Schema validation failed while validating {parameters['data']} file")