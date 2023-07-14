#!/usr/bin/env python
# coding: utf-8

# ### **Librerias necesarias**

# In[22]:


import os
import json
from datetime import datetime, timedelta, date
import requests
import pandas as pd


# In[23]:


# Leer el archivo de credenciales
with open('credentials.json') as file:
    credentials = json.load(file)


#  ## **<u>Conectar a la API</u>**
# - GUardar el df resultante, sin ningun tipo de transformacion, en un archivo .csv

# In[24]:


def connect_to_api():
    # Parámetros de la búsqueda
    query = '''ucrania OR Ukraine OR Ukraine OR Ucraina'''  # palabras clave para efectuar la búsqueda en la API

    today = datetime.today()
    yesterday = today - timedelta(days=1)  # Obtener la fecha del día anterior
    from_date = yesterday.strftime('%Y-%m-%d')
    results_limit = 100
    api_key = credentials['api_key']
    languages = ['es', 'en', 'fr', 'it']

    # Función para realizar la conexión a la API
    def get_articles(url):
        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print('Error al conectarse a la API:', e)
            return []
        else:
            data = response.json()
            articles = data['articles']
            return articles

    # Crear una lista de diccionarios con los datos de cada artículo
    articles_list = []
    for lang in languages:
        url = f"https://newsapi.org/v2/everything?q={query}&language={lang}&from={from_date}&pageSize={results_limit}&apiKey={api_key}"
        articles = get_articles(url)
        for article in articles:
            article_data = {
                'author': article['author'],
                'title': article['title'],
                'description': article['description'],
                'url': article['url'],
                'publishedAt': article['publishedAt'],
                'source': article['source']['name'],
                'language': lang
            }
            articles_list.append(article_data)


    df = pd.DataFrame(articles_list)  # Crear el DataFrame a partir de la lista

    # Se guardará un .csv distinto por día, sin procesar.
    destination_folder = '/opt/airflow/raw_csvs'
    yesterday = date.today() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    file_name = os.path.join(destination_folder, f"news_{date_str}.csv")
    df.to_csv(file_name, index=False)


# In[ ]:




