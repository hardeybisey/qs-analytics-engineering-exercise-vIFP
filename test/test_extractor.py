import pytest
import requests
from unittest.mock import patch, Mock
import pandas as pd
from utils.data_extractor import CSVExtractor, APIExtractor


@pytest.fixture
def mock_requests_get():
    with patch('requests.get') as mock_get:
        yield mock_get
        

@pytest.fixture
def csv_sample():
    data = {
        "glass_type": ["Cocktail Glass", "Shot Glass", "Highball Glass"],
        "stock": [8, 31, 37],
        "bar": ['Budapest', 'New York', 'London'],
    }
    df = pd.DataFrame(data)
    return df


@pytest.fixture
def api_sample():
    data = {"glass":
        ["Cocktail Glass", "Old-Fashioned Glass", "Whiskey Glass"]}
    df = pd.DataFrame(data)
    return df


def test_csv_extractor(csv_sample):
    df = CSVExtractor(
        name = "sample",
        pandas_kwargs = {
            "filepath_or_buffer": "test/sample.csv", 
            "date_format": "%Y-%m-%d %H:%M:%S",
            },
        capitalize_columns = ["bar", "glass_type"]   
    ).fetch_data()
    assert df.shape == (3, 3)
    pd.testing.assert_frame_equal(df, csv_sample)


def test_api_extractor(api_sample,mock_requests_get):   
    response = {
        "drinks": [
            {'strGlass': 'Cocktail glass'},
            {'strGlass': 'old-fashioned Glass'},
            {'strGlass': 'Whiskey glass'},
            ]
        }

    api = APIExtractor(
        name= "sample",
        request_obj= {
            "url": "https://api.coindesk.com/v1/bpi/currentprice.json",
            },
        columns_mapping = {"strGlass":"glass"},
        capitalize_columns = ["glass"],
        data_field= "drinks",
        )
    
    mock_response = Mock()
    mock_response.json.return_value = response
    mock_requests_get.return_value = mock_response
    
    df = api.fetch_data()
    assert df.shape == (3, 1)
    pd.testing.assert_frame_equal(df, api_sample)