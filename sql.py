bar_table = """
CREATE TABLE bars (
    id SERIAL PRIMARY KEY,
    bar TEXT NOT NULL UNIQUE
);
"""

glass_table = """
CREATE TABLE glasses (
    id SERIAL PRIMARY KEY,
    glass TEXT NOT NULL UNIQUE
);
"""

cocktail_table = """
CREATE TABLE cocktails (
    id SERIAL PRIMARY KEY,
    glass_id INTEGER NOT NULL,
    drink TEXT NOT NULL UNIQUE,
    CONSTRAINT fk_glasses
        FOREIGN KEY(glass_id) 
            REFERENCES glasses(id)
);
"""

stock_table = """
CREATE TABLE bar_stock (
    id SERIAL PRIMARY KEY,
    bar_id INTEGER NOT NULL,
    glass_id INTEGER NOT NULL,
    stock INTEGER NOT NULL,
    CONSTRAINT fk_bars
        FOREIGN KEY(bar_id) 
            REFERENCES bars(id),
    CONSTRAINT fk_glasses
        FOREIGN KEY(glass_id) 
            REFERENCES glasses(id)
);
"""

transaction_table = """
CREATE TABLE fact_transactions (
    id SERIAL PRIMARY KEY, 
    bar_id INTEGER NOT NULL,
    cocktail_id INTEGER NOT NULL,
    amount float NOT NULL,
    date timestamp NOT NULL,
    CONSTRAINT fk_bars
        FOREIGN KEY(bar_id) 
            REFERENCES bars(id),
    CONSTRAINT fk_cocktails
        FOREIGN KEY(cocktail_id) 
            REFERENCES cocktails(id)
);
"""

date_table = """
CREATE TABLE dim_date (
    id TEXT PRIMARY KEY,
    date DATE NOT NULL,
    day_name TEXT NOT NULL,
    calender_day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    calender_week INTEGER NOT NULL,
    calender_month INTEGER NOT NULL,  
    quarter INTEGER NOT NULL,  
    year INTEGER NOT NULL
);
"""

bar_insert = """
MERGE INTO bars T
USING 
    (SELECT DISTINCT bar 
    FROM {temp_table}) S
ON T.bar = S.bar
WHEN NOT MATCHED THEN
	INSERT (bar) 
    VALUES (S.bar);
"""

glass_insert = """
MERGE INTO glasses T
USING {temp_table} S
ON T.glass = S.glass
WHEN NOT MATCHED THEN 
	INSERT (glass) 
    VALUES (S.glass);
"""

cocktail_insert = """
MERGE INTO cocktails T
USING 
    (SELECT g.id AS glass_id , c.drink 
    FROM {temp_table} c
    LEFT JOIN glasses g
    ON c.glass=g.glass
    ) S
ON T.glass_id = S.glass_id AND T.drink = S.drink
WHEN NOT MATCHED THEN 
    INSERT (glass_id, drink) 
    VALUES (S.glass_id, S.drink);
"""

stock_insert = """
MERGE INTO bar_stock T
USING 
    (SELECT b.id AS bar_id, g.id AS glass_id, stock 
    FROM {temp_table} ts
    LEFT JOIN bars b
    ON ts.bar=b.bar
    LEFT JOIN glasses g
    ON ts.glass_type=g.glass
    ) S
ON T.bar_id = S.bar_id AND T.glass_id = S.glass_id
WHEN NOT MATCHED THEN 
    INSERT (bar_id, glass_id, stock) 
    VALUES (S.bar_id, S.glass_id, S.stock);
"""
    
transaction_insert = """
INSERT INTO fact_transactions (bar_id, cocktail_id, amount, date)
SELECT b.id AS bar_id, c.id AS cocktail_id, amount, tx.time
    FROM {temp_table} tx
    INNER JOIN bars b ON tx.location=b.bar
    INNER JOIN cocktails c ON tx.drink=c.cocktail
"""