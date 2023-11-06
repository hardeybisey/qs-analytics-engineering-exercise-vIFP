import pytest
import pandas as pd
from utils.data_extractor import CSVExtractor, APIExtractor

@pytest.fixture(scope="module")
def sample_csv_data(tmp_dir):
    data = {
        "stock": [1, 2, 3],
        "date": ["2020-01-01", "2020-01-02", "2020-01-03"],
        "amount": [1.0, 2.0, 3.0],
    }
    path = tmp_dir / "sample.csv"
    df = pd.DataFrame(data)
    df.to_csv(tmp_dir / "sample.csv", index=False)
    return path

def test_csv_extractor(sample_csv_data):
    df = CSVExtractor(
        "name": "sample",
        "pandas_kwargs" :{
            "filepath_or_buffer": "sample.csv", 
            "date_format": "%Y-%m-%d",
            },
        "columns": ["stock", "date", "amount"],
        "capitalize_columns": ["stock"]
        ).fetch_data()
        
    assert df.shape == (3, 3)
    assert df.columns == ["stock", "date", "amount"]
    pd.testing.assert_frame_equal(df, sample_csv_data)
    
    
def test_api_extractor():   
    df = APIExtractor(
        "name": "sample",
        "request_obj" :{
            "url": "https://api.coindesk.com/v1/bpi/currentprice.json",
            },
        "data_field": "test"
        "columns_mapping": {"date":"amount"},
        "capitalize_columns": ["stock"]
        ).fetch_data()
        
    assert df.shape == (1, 2)
    assert df.columns == ["date", "amount"]
    assert df["date"] == "2020-01-01"
    assert df["amount"] == 1.0