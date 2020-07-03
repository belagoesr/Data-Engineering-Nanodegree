from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'isabela',
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_redshift_tables = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=[
        SqlQueries.create_artists_table, 
        SqlQueries.create_songplays_table,
        SqlQueries.create_songs_table,
        SqlQueries.create_staging_events_table,
        SqlQueries.create_staging_songs_table,
        SqlQueries.create_users_table,
        SqlQueries.create_time_table
    ]
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift", 
    aws_credentials_id="aws_credentials",
    table='staging_events',
    region='us-west-2',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path="log_json_path.json",
    access_key_id=AWS_KEY,
    secret_access_key=AWS_SECRET,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift", 
    aws_credentials_id="aws_credentials",
    table='staging_songs',
    region='us-west-2',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_path='auto',
    access_key_id=AWS_KEY,
    secret_access_key=AWS_SECRET,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table='users',
    sql_query=SqlQueries.user_table_insert,
    append=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table='songs',
    sql_query=SqlQueries.song_table_insert,
    append=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table='artists',
    sql_query=SqlQueries.artist_table_insert,
    append=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    table='time',
    sql_query=SqlQueries.time_table_insert,
    append=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    tables=[
        'songplays', 
        'users', 
        'songs', 
        'artists', 
        'time'
    ]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table

load_songplays_table >> [
    load_user_dimension_table, 
    load_song_dimension_table, 
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_quality_checks >> end_operator 
