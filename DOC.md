## Data Errors
==========
- Some cocktails in the transaction files are not available in the cocktails database API. 
- London bar has copper mug spelt wrong in the bar_data csv file. 
- New York bar has a an error in the stock data for highball glass.
- There is a lot of inconsistency in the capitalisation of the glass and cocktail names.

## Config.yaml
======
This file serves as the central configuration file for our pipeline. Within this file, we define essential parameters for configuring the data flow. It serves as  the blueprint for the data's journey, encompassing data source definitions and the specific transformations to be applied to them. Additionally, it holds parameters for our database and tables.

** Structure: **
The file comprises three main sections, each of which plays a pivotal role in orchestrating the data processing pipeline:

- API: Under this section, we define data sources from various APIs. It includes the specifications, parameters, and any custom configurations necessary to fetch data from these sources and process them into a pandas data frame.

- CSV: In this segment, we specify the data sources from CSV files. Similar to the API section, it holds parameters and custom configurations required for handling CSV data into a pandas data frame.

- Database: The database  name and specific tables needs to be defined in this section of the file. 

# Example parameters for CSV and API extraction
```
wwwww
```
## Test
=====
This is where all the utility functions are tested to make sure they produce the expected results when they get the correct input. 


## Steps to run the pipeline

#### Export Required Parameters 
```
export DB_USER=""
export DB_PASSWORD=""
export DB_HOST=""
export DB_PORT=""
export DB_NAME=""
```

#### Create database tables
```
python utils/sql.py 
```

#### Run the pipeline
```
python main.py 
```

## recomendations

- We talk to the data team if it they can be consistent with the format 
- we build our data pipeline with the format of our input data


## Improvements 
=========
Due to time constraint, I was unable to fully implement the time dimension into our data model. The inclusion of a time dimension is of utmost importance in data model design, as it significantly enhances our ability to conduct in-depth analyses. Given more time and context, my approach would involve designing the dimensions as Slowly Changing Dimension (SCD) type 2. This would enable us to capture changes in dimension data and retain historical information.

Implementing SCD type 2 is invaluable for trend analysis, allowing us to answer questions such as how stock levels in a store change over time and which specific products are more abundant at different points in time.


# data issue found during validation
1) Dates are in different format in different stores
2) One of the rows in the bar data has incorrect entry, this might be caused during data entry
