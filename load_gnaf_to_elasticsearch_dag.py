from __future__ import print_function
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

import boto3
import pendulum
import pandas as pd
import tempfile
import datetime as dt
import sys
import logging
from elasticsearch import Elasticsearch
import json
import time
import os
import shutil
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import pandas as pd
import tempfile
from datetime import datetime
from airflow_py.src.utils import DagHelper as dh
import dask.dataframe as da
import ast

local_tz = pendulum.timezone("Australia/Sydney")
target_dbase = "chu_aws_postgres" # eg "chu_aws_postgres"
sched_start = datetime(2020, 8, 3, tzinfo=local_tz)

dag_name = 'Function-load_gnaf_to_elasticsearch'
bucketname = Variable.get("chu-dwh-export-envag")
es_endpoint = Variable.get("es_endpoint_envag")


args = {
    'owner': 'jt',
    'start_date': sched_start,
    'depends_on_past': False,
    'email': ['dwh@chu.com.au'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'on_failure_callback': dh.on_failure_callback,
}


dag = DAG(
    dag_id=dag_name,
    default_args=args,
    # schedule_interval=dh.get_sched_int(dag_name),
    schedule_interval=None,
    concurrency=3, 
    max_active_runs=1,
    tags=[dag_name]
)

get_log_id_task = PythonOperator(
    task_id='get_log_id',
    python_callable=dh.get_log_id,
    op_kwargs={'job_name':dag_name},
    dag=dag
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable=dh.start_func,
    op_kwargs={'job_name':dag_name},
    dag=dag
)

end_task = PythonOperator(
    task_id='end_task',
    python_callable=dh.end_func,
    op_kwargs={'job_name':dag_name},
    dag=dag
)

def remove_folder(folder):
        #Tidy up temporary parquet folder
    try:
        shutil.rmtree(folder)
    except OSError:
        print ("Deletion of the directory %s failed" % folder)
    else:
        print ("Successfully deleted the directory %s" % folder)

def convert_dict_val_to_obj(my_dict):
    conv_dict = {'str':str,'int':'Int64','object':object}
    new_dict = {}
    for key, value in my_dict.items():
        try:
            new_dict[key]=conv_dict[value]
        except KeyError:
            pass
    return new_dict
def export_pg_pq(id,s3_directory,in_sql_table,in_sql_file_num,es_schema,date_columns):
    #Exports postgres table to csv then converts it to parquet, loads it up to s3, then loads it to elasticsearch
    with tempfile.NamedTemporaryFile() as csv_tmp:
        delim = '|'
        in_dbase = target_dbase
        datekey = str(dt.datetime.today().strftime('%Y%m%d%H%M%S'))
        
        s3_directory = s3_directory+f'{datekey}/'
        objectkey = s3_directory+f'btv-{datekey}.gz.parquet'
        limit_val = str(int(in_sql_file_num)-1)
        in_sql = f'select * from {in_sql_table} order by address_detail_pid asc limit (select count(*)/15 from {in_sql_table}) offset (select count(*)/15*{limit_val} from {in_sql_table}) '
        print('downloading / reading csv')
        print(in_dbase,csv_tmp,in_sql,delim)
        dh.download_csv_copy(in_dbase,csv_tmp,in_sql,delim)

        logging.info(es_schema)
        # logging.info(type(es_schema))
        dtype_dict = json.loads(es_schema)
        dtype_dict = convert_dict_val_to_obj(dtype_dict)
        if date_columns:
            date_columns = ast.literal_eval(date_columns)
            df = pd.read_csv(csv_tmp, sep = delim, dtype = dtype_dict,parse_dates=date_columns)
        else:
            df = pd.read_csv(csv_tmp, sep = delim, dtype = dtype_dict)
        print(df.info())
        filelen = len(df.index)
        if filelen == 0:
            raise ValueError('ERROR: filelen == 0 aborting...')
        else:
            # create temp folder for parquet files
            pq_tmp_folder = f'./pq_tmp_folder_{id}_{datekey}/'
            pq_tmp_folder = f'/tmp/pq_tmp_folder_{id}_{datekey}'
            
            os.mkdir(pq_tmp_folder)
            try:
                os.mkdir(pq_tmp_folder)
            except OSError:
                print ("Creation of the directory %s failed" % pq_tmp_folder)
            else:
                print ("Successfully created the directory %s " % pq_tmp_folder)

            # convert dataframe to many parquet files with n_partition rows
            logging.info("Converting datagram to parquet files...")

            ddf = da.from_pandas(df, chunksize=2000)
            schema = pa.Table.from_pandas(ddf._meta_nonempty).schema
            print('schema : \n',schema)
            print('creating parquet files...')
            ddf.to_parquet(pq_tmp_folder,compression='gzip', engine = 'fastparquet', schema = schema)
            return {'pq_tmp_folder':pq_tmp_folder,'filelen':filelen,'s3_directory':s3_directory}

def uploadDirectory(s3_client,path,bucketname,s3_directory):
    connection = BaseHook.get_connection('SSEKMSKeyId_chu-dwh-export-dev')
    extras = eval(connection.extra)
    SSEKMSKeyId=extras['SSEKMSKeyId']
    for root,dirs,files in os.walk(path): 
        print(root,dirs,files)
        for file in files:
            objectkey = s3_directory+file
            s3_client.upload_file(root+'/'+file, bucketname, objectkey, ExtraArgs={"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": SSEKMSKeyId})

def load_to_s3(bucketname,export_pg_pq_task_id,**context):
    export_pg_pq_task_id_dict = context['task_instance'].xcom_pull(task_ids=export_pg_pq_task_id)
    pq_tmp_folder = export_pg_pq_task_id_dict['pq_tmp_folder']
    s3_directory = export_pg_pq_task_id_dict['s3_directory']
    logging.info(f"Making connection to s3...Loading Files to {s3_directory}...")
    s3_client = boto3.client('s3')
    uploadDirectory(s3_client,pq_tmp_folder,bucketname,s3_directory)
    

def tidy_tmp_pq_folder(export_pg_pq_task_id,**context):
    folder = context['task_instance'].xcom_pull(task_ids=export_pg_pq_task_id)['pq_tmp_folder']
    # remove_folder(folder)
    print(folder)

def get_es_count(es_index_name):
    logging.info("Making connection to Elastic Search")
    es = Elasticsearch(
        [es_endpoint],
        scheme="https",
        port=443
    )

    logging.info("Elastic Search connected")
    logging.info("Pre Load Count: ")
    es.indices.refresh(es_index_name)
    count_response = es.cat.count(es_index_name, params={"format": "json"}, request_timeout=30)
    count = int(count_response[0]['count'])
    return count

def escape_str(a_string):
    escaped = a_string.translate(str.maketrans({"-":  r"\-",
                                          "]":  r"\]",
                                          "\\": r"\\",
                                          "^":  r"\^",
                                          "$":  r"\$",
                                          "*":  r"\*",
                                          ".":  r"\.",
                                          "\"": r'\"'}))
    print(escaped)
    return escaped

def load_es(bucketname,export_pg_pq_task_id,es_index_name,es_schema,**context):
    #Lambda/upload to ES part
    lambda_client = boto3.client('lambda')
    export_pg_pq_task_id_dict = context['task_instance'].xcom_pull(task_ids=export_pg_pq_task_id)
    s3_directory = export_pg_pq_task_id_dict['s3_directory']

    logging.info("Preparing Queue...")
    logging.info('''"s3_bucket_name": "{0}",
        "s3_directory": "{1}",
        "es_index_name": "{2},
        "es_schema": "{3}""'''.format(bucketname,s3_directory,es_index_name,es_schema))
    # raise Exception('Not yet') 
    response = lambda_client.invoke(
        FunctionName='dwh-prep-queue',
        Payload='''{{
        "s3_bucket_name": "{0}",
        "s3_directory": "{1}",
        "es_index_name": "{2}",
        "es_schema": "{3}"
        }}'''.format(bucketname,s3_directory,es_index_name,escape_str(es_schema)),
    )
    print('Queue Response : ',response)

    

def wait_es(es_index_name,export_pg_pq_task_id,get_pre_load_es_count_task_id,**context):

    filelen = context['task_instance'].xcom_pull(task_ids=export_pg_pq_task_id)['filelen']
    pre_count = context['task_instance'].xcom_pull(task_ids=get_pre_load_es_count_task_id)
    logging.info('Preload count : '+str(pre_count))

    post_count = get_es_count(es_index_name)
    logging.info("Post Load Count: "+str(post_count))

    print('Waiting on Queue to Complete...')
    numruns = 0
    diff = post_count - pre_count
    print('diff : ',str(diff))
    print('filelen : ',str(filelen))
    sleep_amount = 180
    while diff != filelen:
        print(f'{numruns} minutes.')
        numruns += 1
        if numruns == 10:
            raise Exception('30 minutes have passed, something likely went wrong. Please use the aws dashboard to check on errors in the qeue.') 
        post_count = get_es_count(es_index_name)
        logging.info("Post Load Count: "+str(post_count))
        diff = post_count - pre_count
        if diff == 0:
            logging.info(f'Waiting a {sleep_amount} seconds before trying again...')
            time.sleep(sleep_amount)
        else:
            if diff != filelen:
                logging.info('Looks like the Queue is still going. There are still records missing from the count...')
                logging.info(f'Waiting a {sleep_amount} seconds before trying again...')
                time.sleep(sleep_amount)
    logging.info('Successfully loaded!')

def recreate_index(es_index_name,alias_name):
    logging.info("Making connection to Elastic Search")
    es = Elasticsearch(
        [es_endpoint],
        scheme="https",
        port=443
    )

    logging.info("Elastic Search connected")
    logging.info("Deleting index: "+es_index_name)
    es.indices.delete(index=es_index_name, ignore=[400, 404],request_timeout=60)
    logging.info("Creating index: "+es_index_name)
    es.indices.create(index=es_index_name,request_timeout=60)

def swap_alias(id,new_es_index_name,old_es_index_name,alias_name):
    logging.info("Making connection to Elastic Search")
    es = Elasticsearch(
        [es_endpoint],
        scheme="https",
        port=443
    )

    logging.info("Elastic Search connected")
    logging.info("Adding index alias: "+new_es_index_name)
    #add alias
    body = {
        "actions": [
            {"add": {"index": new_es_index_name, "alias": alias_name}}
        ]
    }
    es.indices.update_aliases(body,request_timeout=60)
    #remove alias
    logging.info("Deleting index alias: "+old_es_index_name)
    body = {
        "actions": [
            {"remove": {"index": old_es_index_name, "alias": alias_name}}
        ]
    }
    es.indices.update_aliases(body,request_timeout=60)
    
    #update table with new index information
    dh.runSql(dh.target_dbase,f"update cfg.pg_to_elasticsearch_tables set es_index_name = \'{old_es_index_name}\', existing_es_index_name = \'{new_es_index_name}\' where id = {id}")

def add_alias(es_index_name,alias_name):
    logging.info("Making connection to Elastic Search")
    es = Elasticsearch(
        ['vpc-dwh-es-4abxp6qznztcecvw7eedfokcoi.ap-southeast-2.es.amazonaws.com'],
        scheme="https",
        port=443
    )

    logging.info("Elastic Search connected")
    logging.info("Deleting index: "+es_index_name)
    #add alias
    body = {
        "actions": [
            {"add": {"index": es_index_name, "alias": alias_name}}
        ]
    }
    es.indices.update_aliases(body,request_timeout=60)

print('pg_to_es_dag initializing.....')

id = 70
table_schema = 'gnaf_ref'
table_name = 'mvw_gnaf_arpc'
es_index_name = 'gnaf_202206_rel_20220815_arpc_2' #enter a new value or it will append
elastic_search_index_alias = 'alias-dev-gnaf'
s3_directory = 'gnaf/loads/'
in_sql_table = table_schema+'.'+table_name
es_schema = '{ "address_detail_pid":"str", "street_locality_pid":"str", "locality_pid":"str", "building_name":"str", "lot_number":"str", "level_number":"str", "flat_number":"str", "destination_point":"str", "number_first":"int", "number_last":"int", "firstnumber":"str", "lastnumber":"str", "street_number":"str", "street":"str", "street_name":"str", "street_type_code":"str", "suburb":"str", "state":"str", "postcode":"str", "primary_secondary":"str", "latitude":"float", "longitude":"float", "confidence":"float", "legal_parcel_id":"str", "street_address":"str", "risk_bfrel":"str", "risk_hsrel":"str", "risk_nhrel":"str", "risk_wsrel":"str", "risk_eqrel":"str", "fld_bldg_rating_zone":"str", "relmatch":"str" , "home_wind_zone":"str", "home_wind_band":"str", "home_flood_band":"str", "home_surge_band":"str", "strata_wind_zone":"str", "strata_wind_band":"str", "strata_flood_band":"str", "strata_surge_band":"str" }'

# pre_load_sql_file = '/home/ubuntu/airflow/dags/airflow_sql/elastic_search/gnaf/gnaf-load-pre.sql'
pre_load_sql_file = ''
date_columns = []

task_list = [get_log_id_task,start_task]
if pre_load_sql_file:
    pre_load_sql_task = PythonOperator(
        task_id='pre_load_sql_gnaf',
        python_callable=dh.open_run_sql_file,
        op_kwargs={'dbase':target_dbase,'filename':pre_load_sql_file, 'table_name':'', 'change_type':'insert'},
        dag=dag
    )
    task_list.append(pre_load_sql_task)

for x in range(1,17):
    job_name = f'{table_schema}_{table_name}_'+str(x)
    
    if x==1:
        truncate_es_task = PythonOperator(
            task_id='truncate_es_',
            python_callable=recreate_index,
            op_kwargs={'es_index_name':es_index_name,'alias_name':elastic_search_index_alias},
            dag=dag,
            retries=3,
        )
        task_list.append(truncate_es_task)



    export_pg_pq_task_name = 'export_pg_pq_task_'+job_name
    export_pg_pq_task = PythonOperator(
        task_id=export_pg_pq_task_name,
        python_callable=export_pg_pq,
        op_kwargs={'id':id,'s3_directory':s3_directory,'in_sql_table':in_sql_table,'in_sql_file_num':str(x),'es_schema':es_schema,"date_columns":date_columns},
        dag=dag
    )
    task_list.append(export_pg_pq_task)

    load_to_s3_task = PythonOperator(
        task_id='load_to_s3_task_'+job_name,
        python_callable=load_to_s3,
        op_kwargs={'bucketname':bucketname,'export_pg_pq_task_id':export_pg_pq_task_name},
        dag=dag
    )
    task_list.append(load_to_s3_task)

    tidy_tmp_pq_folder_task = PythonOperator(
        task_id='tidy_tmp_pq_folder_task_'+job_name,
        python_callable=tidy_tmp_pq_folder,
        op_kwargs={'export_pg_pq_task_id':export_pg_pq_task_name},
        dag=dag
    )
    task_list.append(tidy_tmp_pq_folder_task)

    get_pre_load_es_count_task_id = 'pre_load_count_'+job_name
    get_pre_load_es_count_task = PythonOperator(
        task_id=get_pre_load_es_count_task_id,
        python_callable=get_es_count,
        op_kwargs={'es_index_name':es_index_name},
        dag=dag,
        retries=3,
    )
    task_list.append(get_pre_load_es_count_task)

    load_es_task = PythonOperator(
        task_id='load_es_'+job_name,
        python_callable=load_es,
        op_kwargs={'bucketname':bucketname,'export_pg_pq_task_id':export_pg_pq_task_name,'es_index_name':es_index_name,'es_schema':es_schema},
        dag=dag
    )
    task_list.append(load_es_task)

    wait_es_task = PythonOperator(
        task_id='wait_es_'+job_name,
        python_callable=wait_es,
        op_kwargs={'es_index_name':es_index_name,'export_pg_pq_task_id':export_pg_pq_task_name,'get_pre_load_es_count_task_id':get_pre_load_es_count_task_id},
        dag=dag,
        retries=3,
    )
    task_list.append(wait_es_task)

task_list.append(end_task)
print(task_list)
for ind, task in enumerate(task_list):
    if ind > 0:
        task.set_upstream(task_list[ind-1])

print('..... pg_to_es_ganf_dag loaded.')