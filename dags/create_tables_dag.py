from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.configuration import conf
from operators.create_tables import CreateTablesOperator
from airflow.decorators import dag
from airflow.operators.postgres_operator import PostgresOperator
from helpers.create_tables_sql import CreateTableQueries


@dag(
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 8, 8), 
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'catchup': False
    },
    dag_id='create_tables_dag',
    description='Create tables in Redshift',
    schedule_interval='@once',
    max_active_runs=1
)
def create_tables_dag():

    create_staging_events = CreateTablesOperator(
        task_id='create_staging_events',
        table="staging_events",
        select_sql=CreateTableQueries.create_staging_events
    )

    create_staging_songs = CreateTablesOperator(
        task_id='create_staging_songs',
        table="staging_songs",
        select_sql=CreateTableQueries.create_staging_songs
    )

    create_songplays = CreateTablesOperator(
        task_id='create_songplays',
        table="songplays",
        select_sql=CreateTableQueries.create_songplays
    )

    create_users_table = CreateTablesOperator(
        task_id='create_users_table',
        table="users_table",
        select_sql=CreateTableQueries.create_users_table
    )

    create_song_table = CreateTablesOperator(
        task_id='create_song_table',
        table="song_table",
        select_sql=CreateTableQueries.create_song_table
    )

    create_artist_table = CreateTablesOperator(
        task_id='create_artist_table',
        table="artist_table",
        select_sql=CreateTableQueries.create_artist_table
    )

    create_time_table = CreateTablesOperator(
        task_id='create_time_table',
        table="time_table",
        select_sql=CreateTableQueries.create_time_table
    )

    create_staging_events >> create_staging_songs >> create_songplays >> create_users_table >> \
    create_song_table >> create_artist_table >> create_time_table


dag_instance = create_tables_dag()
