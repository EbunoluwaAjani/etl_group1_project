import glob
import logging
import os

import opendatasets as od
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Set up logging
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - '
    '%(levelname)s - %(message)s'
)

load_dotenv()
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
postgres_url = (
    f"postgresql+psycopg2://postgres:{password}@{username}:"
    f"{host}/postgres"
)

engine = create_engine(postgres_url)

dataset = (
    "https://www.kaggle.com/datasets/atharvasoundankar/"
    "global-housing-market-analysis-2015-2024"
)
od.download(dataset)


def extract_file():
    '''
    returns DataFrames from CSV files matching the pattern '*global-*/*.csv'.
    '''
    logging.info('Extracting files...')
    try:
        for file in glob.glob('*global-*/*.csv'):
            df = pd.read_csv(file)
            yield df
    except Exception as e:
        logging.error(f'Error extracting files: {e}')


def transform():
    '''
     Transforms DataFrames extracted by `extract_file` by cleaning column names
     and converting specific columns to numeric.
    '''
    logging.info('Transforming data...')
    try:
        for df in extract_file():
            df.columns = (
                df.columns.str.strip()
                .str.lower().str.
                replace(" ", "_")
            )
            for columns in df.columns:
                if columns.endswith(('(%)', 'index', 'ratio')):
                    df[columns] = pd.to_numeric(df[columns], errors='coerce')
        return df
    except Exception as e:
        logging.error(f'Error transforming data: {e}')


def load():
    '''
    Loads the transformed DataFrames into a postgres database.
    '''
    logging.info('Loading data into database...')
    try:
        transformed_df = transform()
        transformed_df.to_sql(
            'global_housing',
            con=engine,
            if_exists='replace',
            index=False
        )
        logging.info('Data loaded successfully.')
    except Exception as e:
        logging.error(f'Error loading data: {e}')


load()
