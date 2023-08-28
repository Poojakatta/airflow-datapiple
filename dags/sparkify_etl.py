from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pendulum
import os
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

@dag(
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': pendulum.now(),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'catchup': False
    },
    schedule_interval='@hourly',
    description='A simple DAG with a single task (using decorators)',
    catchup=False,
    dag_id='sparkify_etl',
    max_active_runs=1
)
def sparkify_etl():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        s3_bucket='udacity-dend',
        s3_prefix='log_data',
        region='us-west-2',
        table='staging_events',
        copy_options="JSON 's3://udacity-dend/log_json_path.json'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        s3_bucket="udacity-dend",
        s3_prefix="song_data",
        region='us-west-2',
        table="staging_songs",
        copy_options="FORMAT AS JSON 'auto'"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table="songplays",
        select_sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table="users",
        select_sql=SqlQueries.user_table_insert,
        mode="truncate"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table="songs",
        select_sql=SqlQueries.song_table_insert,
        mode="truncate"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table="artists",
        select_sql=SqlQueries.artist_table_insert,
        mode="truncate"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table="time",
        select_sql=SqlQueries.time_table_insert,
        mode="truncate"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=["songplays", "users", "songs", "artists", "time"],
        dq_checks=[
            {"check_sql": "SELECT COUNT(*) FROM users_table WHERE userid is null", "expected_result": 0, "table": "users"},
            {"check_sql": "SELECT COUNT(*) FROM song_table WHERE songid is null", "expected_result": 0, "table": "songs"},
            {"check_sql": "SELECT COUNT(*) FROM artist_table WHERE artistid is null", "expected_result": 0, "table": "artists"},
            {"check_sql": "SELECT COUNT(*) FROM time_table WHERE start_time is null", "expected_result": 0, "table": "time"},
            {"check_sql": "SELECT COUNT(*) FROM songplays WHERE playid is null", "expected_result": 0, "table": "songplays"}
        ]
    )

    end_operator = DummyOperator(task_id="stop_execution")

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table \
    >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks >> end_operator

dag = sparkify_etl()
