#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import re
import numpy as np
import psycopg2
from datetime import datetime, timedelta, date
import pandas as pd


# ## **<u>Limpiar los datos</u>**

# #### ***Declarar algunas variables y funciones***
# 
# Creo 2 funciones para limpiar los textos, quitando simbolos, comas, tildes, parentesis, etc

# ### **Limpieza general del df**
# Pasos a realizar:
# 
# - Eliminar duplicados (hay muchas notas que estan duplicadas pero difieren en el url. Tener eso en cuenta)
# - Pasar palabras a lowercase, convertir las siglas de la columna lenguaje, reordenar las columnas
# - Reemplazar valores vacios por Nan (despues se rellenan)

# ### **Limpieza especifica por columna**
# 
# En este orden: 
# - Elimino parentesis y contenido de celdas en columna source.
# - Reemplazo Nan values, y 'none' en columnas 'description' y 'author'. 
# - En ambos casos, se reemplaza por lo que dice en la columna 'source'
# - Muchos autores figuran asi: 'la nacion (Carlos pagni)'. Dejo solo los nombres que figuran entre parentesis.
# - Ajusto manualmente unos pocos casos, usando replace
# - Ejecuto la funcion limpiar_celdas en columnas seleccionadas
# - Modifico la columna publishedAt, para que quede el formato fecha

# In[ ]:


def clean_data():
    # Obtener la fecha del día anterior
    yesterday = date.today() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    # Construir el nombre del archivo
    file_name = f"news_{date_str}.csv"
    file_path = os.path.join('/opt/airflow/raw_csvs', file_name)
    df = pd.read_csv(file_path)

    df = df.apply(lambda x: x.astype(str).str.lower())

    # Base para reordenar las columnas
    column_order = ['source', 'title', 'description', 'author', 'publishedAt', 'language', 'url']

    def limpiar_celdas(dataframe, columnas):
        patron = r"[^a-zA-ZáéíóúÁÉÍÓÚüÜñÑ\d\s/]"
        for columna in columnas:
            dataframe[columna] = (
                dataframe[columna]
                .apply(lambda x: re.sub(patron, '', str(x)))
            )
            if columna == 'title':
                dataframe[columna] = dataframe[columna].apply(eliminar_tildes)
        return dataframe

    def eliminar_tildes(texto):
        texto_sin_tilde = texto.replace('á', 'a').replace('é', 'e').replace('í', 'i')\
            .replace('ó', 'o').replace('ú', 'u').replace('Á', 'A').replace('É', 'E').replace('Í', 'I')\
            .replace('Ó', 'O').replace('Ú', 'U')
        return texto_sin_tilde

    def eliminar_parentesis(columna):
        patron = r'\((.*?)\)'  # Patrón de expresión regular para encontrar el texto entre paréntesis
        columna = columna.apply(lambda x: re.sub(patron, '', x)).str.strip()
        # Eliminar el texto entre paréntesis y eliminar espacios en blanco
        return columna

    # Limpieza general del df
    news_cleaned = df.drop_duplicates(subset=['source', 'title', 'description', 'author', 'publishedAt', 'language'])
    news_cleaned = news_cleaned.apply(lambda x: x.astype(str).str.lower())
    news_cleaned = news_cleaned.replace('', np.nan)
    news_cleaned = news_cleaned.reindex(columns=column_order)
    siglas_a_idiomas = {
        'en': 'english',
        'fr': 'french',
        'it': 'italian',
        'es': 'spanish'
    }
    news_cleaned['language'] = news_cleaned['language'].map(siglas_a_idiomas)

    # Limpieza específica por columna
    news_cleaned['source'] = eliminar_parentesis(news_cleaned['source'])
    news_cleaned['description'] = news_cleaned['description'].fillna('none')
    news_cleaned['author'] = news_cleaned['author'].fillna(news_cleaned['source'])
    mask = news_cleaned['author'].eq('none')
    news_cleaned['author'] = news_cleaned['author'].where(~mask, news_cleaned['source'])
    mask = news_cleaned['author'].str.contains(r'\([^)]+\)')
    news_cleaned.loc[mask, 'author'] = news_cleaned.loc[mask, 'author'].str.extract(r'\(([^)]+)\)', expand=False)
    news_cleaned['author'] = (
        news_cleaned['author']
        .str.replace('rt en español\n', 'rt en español')
        .str.replace('https//www.facebook.com/bbcnews', 'bbcnews')
        .str.replace('rt en español , rt en español', 'rt en español')
    )
    columnas_limpiar = ['source', 'title', 'description', 'author']
    news_cleaned = limpiar_celdas(news_cleaned, columnas_limpiar)
    news_cleaned['publishedAt'] = pd.to_datetime(news_cleaned['publishedAt'])
    news_cleaned['publishedAt'] = news_cleaned['publishedAt'].dt.date

    # Directorio de destino para los datos limpios
    cleaned_data_folder = '/opt/airflow/cleaned_csvs'

    # Nombre del archivo de salida
    cleaned_file_name = f"news_{date_str}.csv"

    # Ruta completa del archivo de salida
    cleaned_file_path = os.path.join(cleaned_data_folder, cleaned_file_name)

    # Guardar el DataFrame limpio en un archivo CSV
    news_cleaned.to_csv(cleaned_file_path, index=False)

