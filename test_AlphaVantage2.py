import configparser
import os
from datetime import datetime, timedelta
import time
import pandas as pd
import requests
from pgdb import PGDatabase  # Импорт из pgdb.py (уже корректен)

dirname = os.path.dirname(__file__)
config = configparser.ConfigParser()
config.read(os.path.join(dirname, "config.ini"), encoding="utf-8")

# Безопасное чтение списка компаний
import ast
COMPANIES = ast.literal_eval(config["Companies"]["COMPANIES"])
SALES_PATH = config["Files"]["SALES_PATH"]
DATABASE_CREDS = config["Database"]
API_KEY = config["AlphaVantage"]["API_KEY"]

sales_df = pd.DataFrame()
if os.path.exists(SALES_PATH):
    try:
        sales_df = pd.read_csv(SALES_PATH, encoding="utf-8")
        print("Sales data loaded:", sales_df.head())
        os.remove(SALES_PATH)
    except Exception as e:
        print(f"Ошибка при чтении {SALES_PATH}: {e}")

historical_d = {}

def get_daily_data(symbol):
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY
    }

    resp = requests.get(url, params=params)
    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code} for {symbol}")

    data = resp.json()
    if "Time Series (Daily)" not in data:
        raise Exception(f"API error for {symbol}: {data}")

    df = pd.DataFrame.from_dict(data["Time Series (Daily)"], orient="index")
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    df = df.rename(columns={
        "1. open": "Open",
        "4. close": "Close"
    })
    df = df[["Open", "Close"]]
    df["ticker"] = symbol
    return df.reset_index(names="Date")

# Скачиваем данные
for company in COMPANIES:
    try:
        df = get_daily_data(company)
        # Фильтруем последние 7 дней
        week_ago = datetime.now() - timedelta(days=7)
        historical_d[company] = df[df["Date"] >= week_ago]
        print(f"[+] Загружено {len(historical_d[company])} строк для {company}")
        time.sleep(15)  # ограничение по API (5 вызовов в минуту)
    except Exception as e:
        print(f"[!] Ошибка получения данных для {company}: {e}")

# Подключение к БД
try:
    database = PGDatabase(
        host=DATABASE_CREDS["HOST"],
        database=DATABASE_CREDS["DATABASE"],
        user=DATABASE_CREDS["USER"],
        password=DATABASE_CREDS["PASSWORD"],
    )
except Exception as e:
    print(f"Ошибка подключения к базе данных: {e}")
    raise

# Загрузка sales
for _, row in sales_df.iterrows():
    query = "INSERT INTO finance.sales (dt, company, transaction_type, amount) VALUES (%s, %s, %s, %s)"
    values = (datetime.strptime(row['dt'], "%d-%m-%Y").date(), row['company'], row['transaction_type'], row['amount'])
    try:
        database.post(query, values)
    except Exception as e:
        print(f"Ошибка при вставке данных о продажах: {e}")

# Загрузка stock
for company, data in historical_d.items():
    for _, row in data.iterrows():
        query = "INSERT INTO finance.stock (date, ticker, open, close) VALUES (%s, %s, %s, %s)"
        values = (row['Date'].date(), row['ticker'], float(row['Open']), float(row['Close']))
        try:
            database.post(query, values)
        except Exception as e:
            print(f"Ошибка при вставке данных для {company}: {e}")
