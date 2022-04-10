import datetime as dt
import pytz
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args={
    'owner' : 'Owen',
    'start_date' : dt.datetime(2022,4,10,17,27,0, tzinfo=pytz.timezone('Etc/GMT-7')),
    'email' : 'lalitratana@gmail.com'
}

dag = DAG(
    'ETL_Car_Data',
    schedule_interval=dt.timedelta(minutes=5),
    default_args=default_args,
    description='ETL tasks for Car Order DW'
)

clear_dw = BashOperator(
    task_id='clear_dw',
    bash_command='python3 /Users/lalitratanapusdeekul/Documents/Data_Engineering_Project/ETL_Airflow/clear_dw.py',
    dag=dag
)

etl_date_dim = BashOperator(
    task_id='etl_date_dim',
    bash_command='python3 /Users/lalitratanapusdeekul/Documents/Data_Engineering_Project/ETL_Airflow/etl_date_dim.py',
    dag=dag
)

etl_customer_dim = BashOperator(
    task_id='etl_customer_dim',
    bash_command='python3 /Users/lalitratanapusdeekul/Documents/Data_Engineering_Project/ETL_Airflow/etl_customer_dim.py',
    dag=dag
)

etl_product_dim = BashOperator(
    task_id='etl_product_dim',
    bash_command='python3 /Users/lalitratanapusdeekul/Documents/Data_Engineering_Project/ETL_Airflow/etl_product_dim.py',
    dag=dag
)

etl_fact = BashOperator(
    task_id='etl_fact',
    bash_command='python3 /Users/lalitratanapusdeekul/Documents/Data_Engineering_Project/ETL_Airflow/etl_fact.py',
    dag=dag
)

clear_dw >> [etl_date_dim, etl_customer_dim, etl_product_dim] >> etl_fact
