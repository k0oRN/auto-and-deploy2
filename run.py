import configparser
import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from pgdb import PGDatabase

dirname = os.path.dirname(__file__)

config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"))

COMPANIES = eval(config["Companies"]["COMPANIES"])
SALES_PATH = config["Files"]["SALES_PATH"]
DATABASE_CREDS = config["Database"]

sales_df = pd.DataFrame()
if os.path.exists(SALES_PATH):
    sales_df = pd.read_csv(SALES_PATH)
    os.remove(SALES_PATH)


historical_d = {}
for company in COMPANIES:
    historical_d[company] = yf.download(
        company,
        start=(datetime.today() - timedelta(days=7)).strftime("%Y-%m-%d"),
        end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
    ).reset_index()
    historical_d[company]["ticker"] = company  # Добавим тикер, если он нужен в БД


database = PGDatabase(
    host=DATABASE_CREDS["HOST"],
    database=DATABASE_CREDS["DATABASE"],
    user=DATABASE_CREDS["USER"],
    password=DATABASE_CREDS["PASSWORD"],
)

for i, row in sales_df.iterrows():
    query = f"insert into sales values ('{row['dt']}', '{row['company']}', '{row['transaction_type']}', {row['amount']})"
    database.post(query)


for company, data in historical_d.items():
    for i, row in data.iterrows():
        query = f"insert into stock values ('{row['Date']}', '{row['ticker']}', {row['Open']}, {row['Close']})"
        database.post(query)
