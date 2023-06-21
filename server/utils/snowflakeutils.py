import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from utils.constants.snowflakeconstants import *


def save_to_snowflake(job_id: str, file_path: str):
    """
    Save a file to Snowflake
    :param job_id: Job ID
    :param file_path: path of file containing input data
    """
    engine = create_engine(URL(
        account=sf_account,
        user=sf_user,
        password=sf_password,
        database=sf_database,
        schema=sf_schema,
        warehouse=sf_warehouse,
        role=sf_role,
        region=sf_region
    ))
    df = pd.read_csv(file_path)
    total_rows = len(df)
    if total_rows > sf_sql_limit:
        add_factor = 1
        if total_rows % sf_sql_limit == 0:
            add_factor = 0
        for i in range(0, (total_rows // sf_sql_limit) + add_factor):
            df.iloc[i * sf_sql_limit:(i + 1) * sf_sql_limit].to_sql(f'group_out_{job_id}', con=engine,
                                                                    index=False, if_exists='append')
    else:
        df.to_sql(f'group_out_{job_id}', con=engine,
                  index=False, if_exists='replace')
    engine.dispose()
