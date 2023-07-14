#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import re
import numpy as np
import psycopg2
from datetime import datetime, timedelta, date
import pandas as pd


# ## **<u>Conectar con Amazon Redshift</u>**
# #### **<u>Crear la tabla en Redshift, en caso de que no este creada</u>**
# #### **<u>Insertar los datos ya limpios en la tabla</u>**
# Para este ultimo paso, se procede a verificar si existen datos duplicados entre los que ya estan almacenados en la BD en Redshift, y los que estan por insertarse.
# Tambien se crea un contador para tener seguimiento(en caso de quererlo) acerca de cuantas filas se dejan de lado, por ser duplicados

# In[ ]:


def connect_to_redshift():

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
                except (Exception, psycopg2.Error) as error:
                    print('Error al insertar los datos en Amazon Redshift:', error)
            else:
                duplicates_count += 1

        conn.commit()
        print(f"{duplicates_count} datos duplicados no fueron insertados.")

    except (Exception, psycopg2.Error) as error:
        print('Error al conectar a Amazon Redshift:', error)

