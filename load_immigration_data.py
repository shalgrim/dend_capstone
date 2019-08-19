import logging
import sys
from logging import StreamHandler

import pandas as pd
import psycopg2
from constants import (
    CONN_STRING,
    IMMIGRATION_DATA_FILENAMES,
    READ_CHUNK_SIZE,
    WRITE_CHUNK_SIZE,
)
from sqlalchemy import create_engine
from tqdm import tqdm

logger = logging.getLogger('dend_capstone.load_immigration_data')
logging.basicConfig(
    filename='load_immigration_data.log',
    level=logging.INFO,
    format='%(levelname) -10s %(asctime)s %(module)s at line %(lineno)d: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger.addHandler(StreamHandler(sys.stdout))

drop_immigration_table = 'drop table if exists fact_immigration;'

create_immigration_table = """CREATE TABLE IF NOT EXISTS fact_immigration 
(immigration_id SERIAL PRIMARY KEY,
 cicid int NOT NULL,
 i94yr int NOT NULL,
 i94mon int NOT NULL,
 i94cit int,
 i94res int,
 i94port char(3),
 arrdate int,
 i94mode int,
 i94addr char(3),
 depdate int,
 i94bir int,
 i94visa int,
 count int,
 dtadfile varchar,
 visapost char(3),
 occup char(3),
 entdepa char(1),
 entdepd char(1),
 entdepu char(1),
 matflag char(1),
 biryear int,
 dtaddto varchar,
 gender char(1),
 insnum varchar,
 airline char(3),
 admnum varchar,
 fltno varchar,
 visatype char(3)
);
"""

# Create database connections
engine = create_engine(CONN_STRING)  # needed for DataFrame.to_sql
conn = psycopg2.connect(CONN_STRING)
conn.set_session(autocommit=True)
cur = conn.cursor()


def setup_tables(drop_first=False):
    """
    Creates the fact_immigration table, optionally dropping it first to create anew
    :param drop_first:
    :return: None
    """
    if drop_first:
        cur.execute(drop_immigration_table)
    cur.execute(create_immigration_table)


def populate_table():
    """
    Reads through all the immigration sas files, creating a dataframe for each and inserting it into fact_immigration.
    Additionally creates logging statements of how many rows were in each month's data for data quality checks
    :return: None
    """
    for filename in IMMIGRATION_DATA_FILENAMES:
        logger.info(
            f'reading in {filename}'
        )  # the logging statements' main purpose is to record the time of each step

        myiter = pd.read_sas(
            filename, 'sas7bdat', encoding='ISO-8859-1', chunksize=READ_CHUNK_SIZE
        )
        for sub_df in tqdm(myiter):
            try:
                # the inner join ensures when we do june we only get the columns in common
                immigration_df = pd.concat([immigration_df, sub_df], join='inner')
            except NameError:
                immigration_df = sub_df

        # in addition, this logging statement will help check row counts by month
        logger.info(
            f'done reading in {filename}; shape: {immigration_df.shape} (beware june)'
        )

        if 'may16' in filename:  # special case for june
            continue
        else:
            logger.info(f'inserting rows for dataframe of shape {immigration_df.shape}')
            immigration_df.to_sql(
                'fact_immigration',
                engine,
                if_exists='append',
                chunksize=WRITE_CHUNK_SIZE,
                index=False,
                method='multi',
            )
            logger.info('done inserting')
            del immigration_df  # unless we start anew each month it slows to a crawl


def main(drop_first=False):
    """
    creates and populates fact_immigration from the provided immigration data
    :param drop_first: If the user calls this with any truthy value, or puts any argument after on the command line,
    the table will be dropped before it is re-created and filled
    :return: None
    """
    setup_tables(drop_first)
    populate_table()


if __name__ == '__main__':
    try:
        drop_first = sys.argv[1]
        main(drop_first=True)
    except IndexError:
        main(drop_first=False)
