import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv(".env")


def extract_data(dir_arquivo: str) -> pd.DataFrame:
    dataframe = pd.read_csv(dir_arquivo)

    return dataframe


def load_data(dataframe: pd.DataFrame) -> None:
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = int(os.getenv('POSTGRES_PORT'))
    POSTGRES_DB = os.getenv('POSTGRES_DB')

    POSTGRES_DATABASE_URL = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    engine = create_engine(POSTGRES_DATABASE_URL)

    table_name = 'customer_purchasing_behaviors'

    create_table_query = """
    CREATE TABLE IF NOT EXISTS {tbl_name} (
        user_id INTEGER PRIMARY KEY,
        age INTEGER,
        annual_income INTEGER,
        purchase_amount INTEGER,
        loyalty_score NUMERIC(2,1),
        region VARCHAR(15),
        purchase_frequency INTEGER
    )
    """.format(tbl_name=table_name)
    
    # Connect to the database and execute the query
    try:
        with engine.connect() as connection:
            # Execute the create table query
            connection.execute(text(create_table_query))
            connection.commit()  # Commit the transaction

        # Load data into the table
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        
    except Exception as e:
        print(e)


if __name__ == '__main__':
    caminho_arquivo = 'data/customer_purchasing_behaviors.csv'

    dataframe = extract_data(caminho_arquivo)
    load_data(dataframe)


# POSTGRES_USER="postgres"
# POSTGRES_PASSWORD="qwerty1234"
# POSTGRES_HOST="docker_postgresql_database"
# POSTGRES_PORT="5432"
# POSTGRES_DB="docker_test"

    # docker run -d \
    # --name app_database \
    # -e POSTGRES_USER=app_user \
    # -e POSTGRES_DB=app_db \
    # -e POSTGRES_PASSWORD=app_password \
    # -v pgdata:/var/lib/postgresql/data \
    # -p 5432:5432 \
    # postgres 

# $ docker run -d --name postgres --network postgres -e POSTGRES_USER=postgres -e POSTGRES_DB=docker_test -e POSTGRES_PASSWORD=qwerty1234 -v pgdata:/var/lib/postgresql/data -p 5433:5433 postgres