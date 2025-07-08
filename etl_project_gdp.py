import requests
import pandas as pd 
import sqlite3
import json
import logging
from bs4 import BeautifulSoup

logging.basicConfig(
    filename='etl_project_log.txt',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def extract_data(url):
    logging.info("Starting to fetch data from the website.")
    response = requests.get(url)
    if response.status_code != 200:
        logging.error(f"Failed to fetch the page: status {response.status_code}")
        raise Exception("Request failed.")
    soup = BeautifulSoup(response.content, "html.parser")
    tables = pd.read_html(response.text)
    logging.info(f"Found {len(tables)} tables on the page.")
    return tables[1] #assuming the first table is the one we need

def transform_data(df):
    logging.info("Starting data cleaning and transformation.")
    df.columns = [str(col).strip() for col in df.columns]

    df = df.rename(columns={df.columns[0]: "Country", df.columns[1]: "GDP_USD_billion"})

    df = df[["Country", "GDP_USD_billion"]] #keep only relevant columns
    #convert GDP values to float, coerce non-numeric values to NaN
    df["GDP_USD_billion"] = pd.to_numeric(
        df["GDP_USD_billion"].astype(str).str.replace(',',''),
        errors='coerce'
    )
    print("Before dropna:", df[["Country", "GDP_USD_billion"]].head(10))

    #drop rows with invslid/missing GDP
    df = df.dropna(subset = ["GDP_USD_billion"])

    df["GDP_USD_billion"] = df["GDP_USD_billion"].round(2)

    logging.info("Data transformation completed.")
    return df
def load_to_json(df, json_file):
    logging.info(f"Saving data to JSON file: {json_file}")
    df.to_json(json_file, orient="records", indent=4)

def load_to_db(df, db_file):
    logging.info(f"Saving data to SQLite database: {db_file}")
    conn = sqlite3.connect(db_file)
    df.to_sql("Countries_by_GDP", conn, if_exists="replace", index = False)
    conn.commit()
    logging.info("Data successfully saved into table 'Countries_by_GDP'.")
    return conn

def query_top_economies(conn):
    logging.info("Running SQL query for countries with GDP > 100 billion USD.")
    query = "SELECT * FROM Countries_by_GDP WHERE GDP_USD_billion > 100"
    df_result = pd.read_sql_query(query, conn)
    print(df_result)
    logging.info("SQL query executed and result displayed.")

def main():
    try: 
        url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
        raw_data = extract_data(url)
        cleaned_data = transform_data(raw_data)
        load_to_json(cleaned_data, "Countries_by_GDP.json")
        conn = load_to_db(cleaned_data, "World_Economies.db")
        query_top_economies(conn)
        conn.close()
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()