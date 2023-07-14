from datetime import timedelta, date
import os
import json
import psycopg2
import pandas as pd


credentials_path = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(credentials_path, 'credentials.json'), 'r') as f:
    redshift_credentials = json.load(f)


def connect_to_redshift(**context):
    task_instance = context['task_instance']
    # Obtener la fecha del día anterior
    yesterday = date.today() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    # Construir el nombre del archivo
    file_name = f"news_{date_str}.csv"
    file_path = os.path.join('/opt/airflow/cleaned_csvs', file_name)
    df = pd.read_csv(file_path)

    # Conectar a Amazon Redshift
    try:
        conn = psycopg2.connect(
            dbname=redshift_credentials['dbname'],
            host=redshift_credentials['host'],
            port=redshift_credentials['port'],
            user=redshift_credentials['user'],
            password=redshift_credentials['password']
        )
        cursor = conn.cursor()
        print('Conexión exitosa a Amazon Redshift')

        # Crear la tabla en Amazon Redshift
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS news_articles (
            id INT IDENTITY(1, 1),
            source VARCHAR(255),
            title VARCHAR(1000),
            description VARCHAR(1000),
            author VARCHAR(255),
            publishedAt DATE,
            language VARCHAR(7),
            url NVARCHAR(1000),
            CONSTRAINT unique_news UNIQUE (source, title, description, author)
        );
        '''

        cursor.execute(create_table_query)

        # Insertar datos en la tabla
        inserted_count = 0
        duplicates_count = 0

        for index, row in df.iterrows():
            cursor.execute(f'''
                SELECT COUNT(*) FROM news_articles
                WHERE source = %s AND title = %s AND description = %s AND author = %s
            ''', (
                row['source'],
                row['title'],
                row['description'],
                row['author']
            ))
            count = cursor.fetchone()[0]
            if count == 0:
                # Insertar la fila en la tabla
                insert_query = '''
                INSERT INTO news_articles (author, title, description, url, publishedAt, source, language)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                '''
                record = (
                    row['author'],
                    row['title'],
                    row['description'],
                    row['url'],
                    row['publishedAt'],
                    row['source'],
                    row['language']
                )
                try:
                    cursor.execute(insert_query, record)
                    inserted_count += cursor.rowcount
                except (Exception, psycopg2.Error) as error:
                    print('Error al insertar los datos en Amazon Redshift:', error)
            else:
                duplicates_count += 1

        conn.commit()
        print(f"{duplicates_count} datos duplicados no fueron insertados.")
    except (Exception, psycopg2.Error) as error:
        print('Error al conectar a Amazon Redshift:', error)

    task_instance.xcom_push(key='inserted_count', value=inserted_count)

