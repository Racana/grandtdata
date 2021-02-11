from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Pablo',
    'depends_on_past': False,
    'email': ['pablo@falsemail.com']
}

dag = DAG(
    dag_id='grandtdag',
    default_args=default_args,
    schedule_interval='0 9 * * 3', #Every Wednesday at 9am
    tags=['testing'],
    start_date=days_ago(2)
)

obtain_links = BashOperator(
    task_id='obtain_links_planetagrandt',
    bash_command='python3 ~/testingfolder/planetagrandt.py',
    dag=dag,
)

download_data = BashOperator(
    task_id='download_planetagrandt',
    bash_command='python3 ~/testingfolder/planetagrandt.py',
    dag=dag
)

save_data = BashOperator(
    task_id='save_planetagrandt',
    bash_command='python3 ~/testingfolder/planetagrandt.py',
    dag=dag
)

obtain_links >> download_data >> save_data