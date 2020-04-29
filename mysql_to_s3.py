from datetime import timedelta, datetime

from io import StringIO
from io import BytesIO
import csv

from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.utils.email import send_email
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook 
from airflow.hooks.mysql_hook import MySqlHook 

def read_form_msql():
    mysql_hook = MySqlHook(mysql_conn_id = "aroha_awsrds_mysql_conn")    
    table_name = Variable.get("TABLE_NAME")
    query = """SELECT * """
    query += f'FROM {table_name};'
  
    print(query)
    conn = mysql_hook.get_conn()
    with conn.cursor() as cursor:
        cursor.execute(query)
        with StringIO() as csv_buffer:
            writer = csv.writer(csv_buffer, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(col[0] for col in cursor.description)   
            writer.writerows(cursor)
            data_csv = csv_buffer.getvalue()
    return data_csv

def write_to_s3(data_csv):
    hook = S3Hook(aws_conn_id="aroha_s3_conn")
    key = Variable.get("S3_FILE_NAME")
    data =  data_csv
    bucket_name =  Variable.get("S3_BUCKET_NAME")
    hook.load_string(string_data=data, key=key, replace=True, bucket_name=bucket_name)

def push_data_to_s3_from_mysql():
    data_csv = read_form_msql()
    print(data_csv)
    write_to_s3(data_csv)


def notify_email(contextDict, **kwargs):
    """Send custom email alerts."""

    # email title.
    title = "Airflow alert: {task_name} Failed".format(**contextDict)

    # email contents
    body = """
    Hi Everyone, <br>
    <br>
    There's been an error in the {task_name} job.<br>
    <br>
    Forever yours,<br>
    Airflow bot <br>
    """.format(**contextDict)

    send_email('you_email@address.com', title, body)


default_args = {
    "owner" : "selvam",
    "depends_on_past" : False,
    "start_date": datetime(2020, 4, 29),
    "email":["selvaraj.shanmugam@swirecnco.com"],
    "email_on_failure" : False,
    "email_on_retry" : False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "scedule_interval": "@once"
}

with DAG("mysql_to_s3", default_args=default_args) as dag:

    t1 = BashOperator(
        task_id = "print_task",
        bash_command='echo "From mysql to s3!!"',
    )


    t2 = PythonOperator(
        task_id = "mysql_to_s3",
        python_callable = push_data_to_s3_from_mysql,
        on_failure_callback=notify_email,
    )


t1 >> t2
