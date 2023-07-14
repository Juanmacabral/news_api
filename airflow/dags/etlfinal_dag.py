from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from utils.extract import connect_to_api
from utils.transform import clean_data
from utils.load import connect_to_redshift
import smtplib
from airflow.models.taskinstance import TaskInstance



default_args = {
    'owner': 'juanmacabral_coderhouse',
    'start_date': datetime(2023, 7, 12),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}




dag = DAG(
    'etlfinal_dag',
    default_args=default_args,
    schedule_interval='@daily',
    concurrency=1
)

task1 = PythonOperator(
    task_id='connect_api',
    python_callable=connect_to_api,
    provide_context=True,
    dag=dag
)


task2 = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    provide_context=True,
    dag=dag
)

task3 = PythonOperator(
    task_id='connect_redshift',
    python_callable=connect_to_redshift,
    provide_context=True,
    dag=dag
)


today = datetime.today()
yesterday = today - timedelta(days=1)  # Obtener la fecha del día anterior
from_date = yesterday.strftime('%Y-%m-%d')


def enviar(task_instance: TaskInstance):
    rows_rawdata = task_instance.xcom_pull(task_ids='connect_api', key='rows_rawdata')
    inserted_count = task_instance.xcom_pull(task_ids='connect_redshift', key='inserted_count')
    final = f'El dia {from_date} se han recibido {rows_rawdata} noticias relacionadas con Ucrania.' \
            f' Luego de limpiar la informacion recibida, y eliminar duplicados, han sido insertados {inserted_count} en la base de datos '
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('juanmacabral@gmail.com','fdsuzktsmcfkdxzl')
        subject= f'News from from Ukraine, {from_date} '
        body_text=final
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail('juanmacabral@gmail.com','juanmacabral@gmail.com',message)
        print('Exito')
    except Exception as exception:
        print(exception)
        print('Failure')




task4 = PythonOperator(
    task_id='enviar',
    python_callable=enviar,
    provide_context=True,
    dag=dag
)



task1 >> task2 >> task3 >> task4

