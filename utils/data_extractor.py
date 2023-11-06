import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any as AnyType
import requests
import pandas as pd

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """
    This class defines the common interface for all data extraction methods.
    """

    @abstractmethod
    def fetch_data(self, **kwargs):
        pass


class CSVExtractor(BaseExtractor):
    """
    Extract data from a CSV file with the specified parameters.

    parameters
    ----------
    name : str
        Description for the data source.

    pandas_kwargs : Dict[AnyType,AnyType]
        Parameters passed to pandas.read_csv function.

    columns_mapping : Optional[Dict[AnyType,AnyType]]
        Key, Value pairs of columns that should be renamed.

    capitalize_columns : Optional[List[str]]
        List of columns that should be capitalized.

    drop_columns : Optional[List[AnyType]]
        List of columns that should be dropped from the dataframe.
    """

    def __init__(
        self,
        name: str,
        pandas_kwargs: Dict[AnyType, AnyType],
        capitalize_columns: Optional[List[str]] = None,
        columns_mapping: Optional[Dict[AnyType, AnyType]] = None,
        drop_columns: Optional[List[AnyType]] = None,
    ) -> None:
        self.name = name
        self.pandas_kwargs = pandas_kwargs
        self.capitalize_columns = capitalize_columns
        self.columns_mapping = columns_mapping
        self.drop_columns = drop_columns

    def fetch_data(self) -> Optional[pd.DataFrame]:
        """
        This method reads data from a CSV file using the configurations
        passed to the class constructor and returns it as a DataFrame.

        Returns
        -------
        df: pd.DataFrame
            DataFrame containing extracted data.
        """
        try:
            logger.info(
                "Getting %s data from %s",
                self.name,
                self.pandas_kwargs["filepath_or_buffer"],
            )
            df = pd.read_csv(**self.pandas_kwargs)
            if self.drop_columns:
                df.drop(columns=self.drop_columns, inplace=True)
            if self.columns_mapping:
                df.rename(columns=self.columns_mapping, inplace=True)
            if self.capitalize_columns:  # capitalize all words in the specified columns
                df[self.capitalize_columns] = df[self.capitalize_columns].map(str.title)
            df.drop_duplicates(inplace=True)  # remove duplicates
            logger.info(
                "Finished getting %s data from  %s",
                self.name,
                self.pandas_kwargs["filepath_or_buffer"],
            )
            return df
        except Exception as e:
            logger.error(
                "Error getting %s data with the specified configuration",
                self.name,
                exc_info=True,
            )
            raise e


class APIExtractor(BaseExtractor):
    """
    Extract data from a Web API with the specified parameters.

    parameters
    ----------
    name : str
        Description for the data source.

    request_obj : Dict[AnyType,AnyType]
        Parameters passed to the requests.get function.

    data_field : str
        The key for the data field in the API response.

    capitalize_columns : Optional[List[str]]
        List of columns that should be capitalized.

    columns_mapping : Optional[Dict[AnyType,AnyType]]
        Key, Value pairs of columns that should be renamed.

    drop_columns : Optional[List[AnyType]]
        List of columns that should be dropped from the dataframe.
    """

    def __init__(
        self,
        name: str,
        request_obj: Dict[AnyType, AnyType],
        data_field: str,
        capitalize_columns: Optional[List[str]] = None,
        columns_mapping: Optional[Dict[AnyType, AnyType]] = None,
        drop_columns: Optional[List[AnyType]] = None,
    ):
        self.name = name
        self.request_obj = request_obj
        self.data_field = data_field
        self.capitalize_columns = capitalize_columns
        self.columns_mapping = columns_mapping
        self.drop_columns = drop_columns

    def fetch_data(self) -> Optional[pd.DataFrame]:
        """
        This method reads data from an API using the configurations
        passed to the class constructor and returns it as a DataFrame.

        Returns
        -------
        df: pd.DataFrame
            DataFrame containing extracted data.
        """
        logger.info("Getting %s data from %s", self.name, self.request_obj["url"])
        try:
            resp = requests.get(**self.request_obj)
            resp = resp.json()[self.data_field]
            df = pd.DataFrame(resp)
            if self.drop_columns:
                df.drop(columns=self.drop_columns, inplace=True)
            if self.columns_mapping:
                df.rename(columns=self.columns_mapping, inplace=True)
            df.drop_duplicates(inplace=True)  # remove duplicates
            if self.capitalize_columns:  # capitalize all words in the specified columns
                df[self.capitalize_columns] = df[self.capitalize_columns].map(str.title)
            logger.info(
                "Finished getting %s data from %s", self.name, self.request_obj["url"]
            )
            return df
        except Exception as e:
            logger.error(
                "Error getting %s data with the specified configuration",
                self.name,
                exc_info=True,
            )
            raise e
