from datetime import timedelta
from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import requests 
from psycopg2 import extras

POSTGRES_CONN_ID = 'nikita-belov-cbx8663_pg'
TABLE_NAME = 'chars'


default_args = {
    "owner": "nikita-belov-cbx8663",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "nikita-belov-cbx8663_5",
    default_args = default_args,
    description = "Load from API to PG",
    schedule_interval = "@daily",
    max_active_runs = 1,
    start_date = days_ago(2),
    concurrency = 1
) as dag:
    
    create_table = PostgresOperator (
        task_id = "create_table",
        postgres_conn_id = POSTGRES_CONN_ID,
        sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            name varchar(100),
            status varchar (100),
            species varchar (100),
            type varchar(100),
            gender varchar(100)
        )
        """
    )

    truncate_table = PostgresOperator (
        task_id = "truncate_table",
        postgres_conn_id = 'nikita-belov-cbx8663_pg',
        sql = f"""
        TRUNCATE TABLE {TABLE_NAME}
        """
    )
    def load_to_PG():
        url = "https://rickandmortyapi.com/api/character"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            pages_cnt = int(data.get("info").get("pages"))
            cur_page = 1
            data_to_insert = []

            while cur_page <= pages_cnt:
                response = requests.get(f"{url}?page={cur_page}")
                if response.status_code != 200:
                    break
                data = response.json()
                for i in data['results']:
                    id = i["id"]
                    name = i["name"]
                    status = i["status"]
                    species = i["species"]
                    type = i["type"]
                    gender = i["gender"]

                    data_to_insert.append((id, name, status, species, type, gender))
                cur_page += 1
            pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            insert_query = f"""
                INSERT INTO {TABLE_NAME} (id, name, status, species, type, gender)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
            """
            extras.execute_values(
                cursor,
                insert_query,
                data_to_insert,
            )
            conn.commit()
            cursor.close()
            conn.close()

        else:
            print("Ошибка при исполнении запроса: ", response.status_code)


    save_data_to_pg = PythonOperator(
        task_id = "save_data_to_pg",
        python_callable = load_to_PG
    )

    create_table >> truncate_table >> save_data_to_pg
