import logging
import os
from datetime import datetime, date
import yaml

from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator

from google.cloud import bigquery
from google.oauth2 import service_account

ENV = Variable.get("environment").lower()

INGESTION_PROJECT = f'adw-lake-{ENV}'

SUBNETWORK = f'projects/it-network-project-246216/regions/us-east1/subnetworks/aca-ingest-{"prod" if ENV == "prd" else ENV}-subnet'
REGION = 'us-east1'

SOURCE_DB_CONFIG_FILE_NAME = f'{ENV}_data_source_config.yaml'
DB_CLUSTER_CONFIG_FILE_NAME = f'{ENV}_cluster_config.yaml'

CWD = os.path.dirname(os.path.abspath(__file__))


################
# File Reading #
################

def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def _locate_dir(dir, db):
    """
    Get path of given directory string and given database.
    """
    folderup = CWD.rfind("/")
    config_path = CWD[:folderup] + f"/{dir}/" + db + "/"

    return config_path


def read_webserver_cluster_config(db):
    logging.info(f"""Reading cluster config for {db} ...""")

    config_path = _locate_dir('db_cluster_configs', db)
    filepath = config_path + DB_CLUSTER_CONFIG_FILE_NAME

    config = read_config_file(filepath)

    logging.info(f"""Serialized cluster config: {str(config)}""")

    return config


def read_webserver_datasource_config(db):
    logging.info(f"""Reading db config for {db} ...""")

    config_path = _locate_dir('db_source_configs', db)
    filepath = config_path + SOURCE_DB_CONFIG_FILE_NAME

    config = read_config_file(filepath)

    logging.info(f"""Serialized db config: {str(config)}""")

    return config


def read_webserver_table_config(db, filename):
    logging.debug(f"""Reading table config at {filename} ...""")

    util_path = _locate_dir('table_ingestion_configs', db)
    filepath = util_path + filename

    table_config = read_config_file(filepath)

    logging.debug(f"""Serialized table config: {str(table_config)}""")

    return table_config


def read_webserver_table_file_list(db):
    logging.info(f"""Starting the reading of table configs for database: {db}...""")

    db_source_path = _locate_dir('table_ingestion_configs', db)

    config_list = []
    for file in os.listdir(db_source_path):
        config = read_webserver_table_config(db, file)
        config_list.append(config)

    return config_list


def build_file_location(table_config, dag):
    target_dir = table_config['target_dir']
    target_dir = target_dir.format(ENV=ENV)
    return target_dir


def build_file_partition():
    """
    Determines naming convention of directory that Sqoop writes to and directory that bq load looks for. Since Airflow
    is retriggered every heartbeat, this cannot simply be a timestamp, so we truncate within the last 2 hours.
    """

    dt = date.today().isoformat()
    #hr = int(datetime.now().strftime('%H'))

    # truncate to 2 hour, essentially if odd number hour then revert to previous even hour (5 becomes 4 etc)
    #if hr % 2 == 1:
    #    hr -= 1

    return f'{dt}'


###########
# Cluster #
###########

def create_dataproc_cluster(cluster_config, dag):
    cluster_task_id = "create-cluster-{}".format(cluster_config['cluster_name'])

    task = DataprocClusterCreateOperator(
        project_id=INGESTION_PROJECT,
        subnetwork_uri=SUBNETWORK,
        task_id=cluster_task_id,
        cluster_name=cluster_config['cluster_name'],

        idle_delete_ttl=cluster_config.get('idle_delete_ttl', 10800),

        num_workers=cluster_config['num_workers'],
        master_machine_type=cluster_config['master_machine_type'],
        master_disk_type=cluster_config['master_disk_type'],
        master_disk_size=cluster_config['master_disk_size'],
        worker_machine_type=cluster_config['worker_machine_type'],
        worker_disk_type=cluster_config['worker_disk_type'],
        worker_disk_size=cluster_config['worker_disk_size'],

        region=REGION,
        zone='us-east1-b',
        image_version='1.2-debian9',
        #image_version='1.4-debian9',
        internal_ip_only=True,
        service_account_scopes=[
            "https://www.googleapis.com/auth/devstorage.read_only",
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/monitoring.write",
            "https://www.googleapis.com/auth/pubsub",
            "https://www.googleapis.com/auth/service.management.readonly",
            "https://www.googleapis.com/auth/servicecontrol",
            "https://www.googleapis.com/auth/trace.append",
            "https://www.googleapis.com/auth/sqlservice.admin"
        ],
        properties={'dataproc:dataproc.conscrypt.provider.enable': 'false'},

        dag=dag
    )

    return task


def delete_dataproc_cluster(cluster_config, dag):
    cluster_task_id = "delete-cluster-{}".format(cluster_config['cluster_name'])

    task = DataprocClusterDeleteOperator(
        project_id=INGESTION_PROJECT,
        cluster_name=cluster_config['cluster_name'],
        task_id=cluster_task_id,
        region=REGION,
        dag=dag
    )
    return task


##############################
# Sqoop Command / Submission #
##############################

def _build_db_level_sqoop_command(db_config, cluster_config):
    """
    Populate connection and database level arguments for the Sqoop command. Provides clear location to add conditional
    handling for arguments not always necessary in the db config file like drivers and connection-managers etc.
    """

    # formatting is particular - newlines should have no trailing space, for ease of use always end string with space
    cmd_template = f"""gcloud dataproc jobs submit hadoop \
    --project='{INGESTION_PROJECT}' \
    --cluster='{cluster_config["cluster_name"]}' \
    --region='{REGION}' \
    --class=org.apache.sqoop.Sqoop \
    --jars='{db_config["jar_files"]}' \
    -- import -Dmapreduce.job.user.classpath.first=true \
    --connect='{db_config["connect"]}' \
    --username='{db_config["username"]}' \
    --password-file='{db_config["password_file"]}' """

    if db_config.get('connection_manager'):
        cmd_template += f"""--connection-manager='{db_config["connection_manager"]}' """

    if db_config.get('driver'):
        cmd_template += f"""--driver='{db_config["driver"]}' """

    return cmd_template


def _build_table_level_sqoop_args(dag, db_config, table_config):
    """
    Populate and add table-level arguments for Sqoop command. Provides clear location to add conditional handling
    for arguments specified in the table-config files.
    """

    query = table_config['query'].format(db_name=db_config.get('db_name'),
                                         batch_number=table_config['batch_number']
                                         )

    target_dir = build_file_location(table_config, dag)

    sqoop_args = f"""--query='{query}' --target-dir='{target_dir}' """

    #############################
    # Mapping / Threading Logic #
    #############################
    # if n_mapper >1 then will need split-by-column
    if table_config.get('split_by_column'):
        sqoop_args += f"""--split-by='{table_config["split_by_column"]}' --num-mappers={table_config["num_mappers"]} """

    else:
        sqoop_args += f""" --num-mappers=1"""

    ####################
    # Loading Strategy #
    ####################
    load_strategy = table_config.get('load_strategy', 'append')
    if load_strategy == 'append' and table_config.get('hwm'):
        hwm_col = table_config['hwm_column']
        hwm = table_config['hwm']

        sqoop_args += f" --incremental={load_strategy} --check-column='{hwm_col}' --last-value='{hwm}'  --temporary-rootdir='{target_dir}' "

    ################################
    # File Formatting & Datatypes #
    ################################
    sqoop_args += table_config.get('file_format', ' --as-textfile ')
    sqoop_args += table_config.get('fields-terminated-by', ' --fields-terminated-by="|" ')

    return sqoop_args

def _build_gsutil_delete_cmd(dag, db_config, table_config):
    """
    Populate and add table-level arguments for Sqoop command. Provides clear location to add conditional handling
    for arguments specified in the table-config files.
    """

    target_dir = build_file_location(table_config, dag)

    sqoop_args = f"gsutil rm -r {target_dir}; "

    return sqoop_args


def submit_sqoop_job(cluster_config, db_config, table_config, dag):
    """
    Build Sqoop command and populate Operator
    """

    cmd_delete = _build_gsutil_delete_cmd(dag, db_config, table_config)
    cmd_db_level = _build_db_level_sqoop_command(db_config, cluster_config)
    cmd_table_args = _build_table_level_sqoop_args(dag, db_config, table_config)

    # build final command
    cmd = cmd_delete + cmd_db_level + cmd_table_args

    sqoop_task = BashOperator(
        task_id="sqoop-job-{}".format(table_config['table_name']),
        bash_command=cmd,
        dag=dag,
        priority_weight=1,
        weight_rule='upstream')

    return sqoop_task


def convert_to_utf8(dag, table_config):
    """
    Task that converts csv file from ASCII to UTF8. This is required because 'bq load' expects utf8 format. Pools all
    mapped files into one 'data.csv' for its run-hour directory.
    """
    data_file_loc = build_file_location(table_config, dag)


    #cmd = f'gsutil cp {data_file_loc}/p* - | tr -d "\\000" | gsutil cp - {data_file_loc}/data.csv'

    cmd = f'VAR=$(gsutil ls {data_file_loc}/part-m-00000| wc -l) 2> null && \
           if [ $VAR -gt 0 ] \n \
           then \
               VAR1=$(gsutil cp {data_file_loc}/p* - | tr -d "\\000" | gsutil cp - {data_file_loc}/data.csv) \n \
               if [ $? -ne 0 ] \n \
                 then echo "Error creating udf8 output - data.csv"; echo $VAR1; exit 1 \n \
                 else echo $VAR1 \n \
                fi \n \
           else echo "Sqoop output datafile does not exist" \n \
           fi'

    task_id = f"convert-to-utf8-{table_config['table_name']}"

    return BashOperator(task_id=task_id,
                        bash_command=cmd,
                        dag=dag,
                        priority_weight=2,
                        weight_rule='upstream')



def gcs_to_bq(dag, table_config):
    dataset = table_config['dataset_name']
    table = table_config['table_name']
    hwm = table_config['hwm_column']

    data_file_loc = build_file_location(table_config, dag)

    if table_config['database_source'] in ('d3', 'mzp', 'd3cur'):
        filename = f"{data_file_loc}/data.csv"

    else:
        filename = f"{data_file_loc}/part-m*"

    logging.info(f"Loading Table '{table}' with high water mark value as '{hwm}'")

    #if hwm is None:
    #    cmd = f'bq load --replace --project_id={INGESTION_PROJECT} --source_format=CSV --null_marker="null" --quote="" --field_delimiter="|" {dataset}.{table} "{filename}"'
    #
    #else:
    #    cmd = f'bq load --project_id={INGESTION_PROJECT} --source_format=CSV --null_marker="null" --quote="" --field_delimiter="|" {dataset}.{table} "{filename}"'

    if hwm is None:
        cmd = f'VAR=$((gsutil ls {data_file_loc}/data.csv||gsutil ls {data_file_loc}/part-m-00000)| wc -l) 2> null && \
                if [ $VAR -gt 0 ] \n \
                  then VAR1=$(bq load --replace --project_id={INGESTION_PROJECT} --source_format=CSV --null_marker="null" --quote="" --field_delimiter="|" {dataset}.{table} "{filename}") \n \
                    if [ $? -ne 0 ] \n \
                      then echo "BQ load failed. "; echo $VAR1; exit 1 \n \
                      else echo $VAR1 \n \
                    fi \n \
                  else echo "Applicable sqoop output or utf8 Output datafile does not exist" \n \
                fi'

    else:
        cmd = f'VAR=$((gsutil ls {data_file_loc}/data.csv||gsutil ls {data_file_loc}/part-m-00000)| wc -l) 2> null && \
                if [ $VAR -gt 0 ] \n \
                then VAR1=$(bq load --project_id={INGESTION_PROJECT} --source_format=CSV --null_marker="null" --quote="" --field_delimiter="|" {dataset}.{table} "{filename}") \n \
                    if [ $? -ne 0 ] \n \
                      then echo "BQ load failed. "; echo $VAR1; exit 1 \n \
                      else echo $VAR1 \n \
                    fi \n \
                else echo "Applicable sqoop output or utf8 Output datafile does not exist" \n \
                fi'

    gcs_to_bq_task_id = "gcs-to-bq-{}".format(table)

    return BashOperator(task_id=gcs_to_bq_task_id,
                        bash_command=cmd,
                        dag=dag,
                        priority_weight=3,
                        weight_rule='upstream')

############################################################
# DYNAMIC CONFIGS - Highwatermarks / Control Table / Batch #
# anything with state -------------------------------------#
############################################################

def _get_bq_client():
    """
    Return BigQuery Client using service account credentials stored in Airflow variable.
    """
    key_path = Variable.get("bigquery_ro_service_account")

    credentials = service_account.Credentials.from_service_account_file(
        filename=key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials, project=INGESTION_PROJECT)

    return client


def read_batch_table():
    """
    Return dict of batch information from table `admin.ingestion_batch`
    """
    client = _get_bq_client()

    query = f"SELECT * FROM `adw-lake-{ENV}.admin.ingestion_batch` where batch_status = 'active' "
    query_job = client.query(query, location="US")

    batch_config = {}
    for row in query_job:  # expects [batch_number, batch_status, batch_starttime, batch_endtime]
        batch_number, batch_status, batch_starttime, batch_endtime = row[:4]

        batch_config['batch_number'] = batch_number
        batch_config['batch_status'] = batch_status
        batch_config['batch_starttime'] = batch_starttime
        batch_config['batch_endtime'] = batch_endtime

    return batch_config


def read_hwm_table(db):
    """
    Return dict of highwatermarks for all tables in source-db-schema from `admin.ingestion_highwaters.
    Used to populate table configs and Sqoop commands.
    """

    client = _get_bq_client()

    query = f"SELECT * FROM `adw-lake-{ENV}.admin.ingestion_highwatermarks` where dataset_name = '{db}' "
    query_job = client.query(query, location="US")

    highwatermarks = {}
    for row in query_job:  # expects [dataset_name, table_name, highwatermark_value]
        _, table, hwm = row[:3]
        highwatermarks[table] = hwm

    return highwatermarks


def _inject_highwatermark_config(highwatermark_dict, table_config):
    """
    Isolate logic to update table config dictionary with the highwatermark value.
    """
    table_name = table_config['table_name']
    hwm_value = highwatermark_dict.get(table_name, '')

    logging.info(f"Setting table '{table_name}' with value '{hwm_value}'")

    table_config['hwm'] = hwm_value
    return table_config


def _inject_batch_config(batch_dict, table_config):
    """
    Isolate logic to update table config dictionary with the batch number values.
    """
    table_config.update(batch_dict)
    return table_config


def inject_dynamic_configs(db, table_configs):
    """
    Read any dynamic variables and apply them to the table configurations.
    """
    config_highwatermarks = read_hwm_table(db)
    config_batch = read_batch_table()

    augmented_table_configs = []
    for table_config in table_configs:
        logging.info(f"""AUGMENTING TABLE CONFIG - {table_config['table_name']} - ...""")

        table_config = _inject_highwatermark_config(config_highwatermarks, table_config)
        table_config = _inject_batch_config(config_batch, table_config)

        logging.info(f"""For table "{table_config['table_name']}", using configuration: "{str(table_config)}" """)

        augmented_table_configs.append(table_config)

    return augmented_table_configs


def write_to_hwm_table(dag, table_config):
    dataset = table_config['dataset_name']
    table = table_config['table_name']
    hwm = table_config['hwm_column']

    if dataset == 'd3' and table == 'arch_call':
        hwm = 'cast(DATETIME_ADD(cast(arch_date as datetime), INTERVAL -30 DAY) as string)'
    else:
        hwm = table_config['hwm_column']

    if hwm is None:
        cmd = f"""bq query --project_id={INGESTION_PROJECT} --use_legacy_sql=false "UPDATE admin.ingestion_highwatermarks SET highwatermark = '' WHERE dataset_name = '{dataset}' and table_name = '{table}'" """

    else:
        cmd = f"""bq query --project_id={INGESTION_PROJECT} --use_legacy_sql=false "UPDATE admin.ingestion_highwatermarks SET highwatermark = (select coalesce(cast(max(PARSE_DATETIME('%Y-%m-%d %H:%M:%E*S', {hwm})) as STRING), '') from {dataset}.{table}) WHERE dataset_name = '{dataset}' and table_name = '{table}'" """

    return BashOperator(task_id=f'update-control-table-{dataset}-{table}',
                        dag=dag,
                        bash_command=cmd,
                        retries=20,
                        priority_weight=4,
                        weight_rule='upstream')
