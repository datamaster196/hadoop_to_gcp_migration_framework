from datetime import datetime, timedelta

from airflow import DAG

from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable

DAG_TITLE = 'migrate_history'
SCHEDULE = Variable.get("migrate_history", default_var='') or None

default_args = {
    'owner'           : 'airflow',
    'depends_on_past' : False,
    'start_date'      : datetime(2019, 8, 22),
    'email'           : ['airflow@example.com'],
    'catchup'         : False,
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 0,
    'retry_delay'     : timedelta(minutes=5),
}


def copy_hist(db, table_name):
    cmd = f'''create table `{iu.INGESTION_PROJECT}.{db}.{table_name}_hist` as select * from `adw-lake-dev.{db}.{table_name}_hist`'''
    return int_u.run_query(f"build_{table_name}", cmd)


with DAG(DAG_TITLE, default_args=default_args, schedule_interval=SCHEDULE) as dag:
    for m in iu.read_webserver_table_file_list('mzp'):
        copy_hist('mzp', m['table_name'])

    for e in iu.read_webserver_table_file_list('epic'):
        copy_hist('epic', e['table_name'])

    for d in iu.read_webserver_table_file_list('d3'):
        copy_hist('d3', d['table_name'])
