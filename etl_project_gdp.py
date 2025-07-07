import requests
import pandas as pd 
import sqlite3
import json
import logging
from bs4 import BeautifulSoup

logging.basicConfig(
    filename='etl_project_log.txt'
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
    return tables[0] #assuming the first table is the one we need