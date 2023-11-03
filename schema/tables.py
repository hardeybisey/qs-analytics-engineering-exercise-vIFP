transaction_table = """
CREATE TABLE fact_transactions (
    fact_key SERIAL PRIMARY KEY, /
    bar_key INTEGER NOT NULL,
    cocktail_key INTEGER NOT NULL,
    amount float NOT NULL,
    date timestamp NOT NULL,
);
"""
bar_table = """
CREATE TABLE dim_bar (
    bar_id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
);
"""

glass_table = """
CREATE TABLE dim_glass (
    glass_key SERIAL PRIMARY KEY,
    name TEXT  NOT NULL,
);
"""

cocktail_table = """
CREATE TABLE dim_cocktail (
    cocktail_key SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    glass TEXT NOT NULL,
);
"""

date_table = """
CREATE TABLE dim_date (
    date_id PRIMARY KEY TEXT,
    date DATE NOT NULL,
    day_name TEXT NOT NULL,
    calender_day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    calender_week INTEGER NOT NULL,
    calender_month INTEGER NOT NULL,  
    quarter INTEGER NOT NULL,  
    year INTEGER NOT NULL,
    day_name TEXT NOT NULL,
);
"""