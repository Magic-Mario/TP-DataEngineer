import polars as pl
import numpy as np 
import pandas as pd

#* Cargar los archivos csv como dataframes

amazon_df = pl.read_csv('../Datasets/amazon_prime_titles-score.csv')
disney_plus_df = pl.read_csv('../Datasets/disney_plus_titles-score.csv')
netflix_title_df = pl.read_csv('../Datasets/netflix_titles-score.csv')
hulu_df = pd.read_csv('../Datasets/hulu_titles-score (2).csv')

def fill_na(df):
    """
    Rellena valores NaN en la columna 'rating' con el valor 'G'
    
    Argumentos:
    df -- DataFrame de entrada
    
    Devuelve:
    df -- DataFrame con valores NaN en la columna 'rating' rellenados con 'G'
    """
    df['rating'].fillna(value='G', inplace=True)
    return df


def generate_id(df):
    """
    Genera un ID combinando la primera letra del título y el número de show_id
    
    Argumentos:
    df -- DataFrame de entrada
    
    Devuelve:
    str -- ID generado
    """
    plataform_name = df['title']
    show_id = df['show_id']
    return plataform_name[0] + str(show_id)

def create_id_column(df):
    """
    Crea una columna 'id' en el DataFrame que contiene los IDs generados
    
    Argumentos:
    df -- DataFrame de entrada
    
    Devuelve:
    df -- DataFrame con una columna 'id' que contiene los IDs generados
    """
    df['id'] = df.apply(generate_id, axis=1)
    pop_id = df.pop('id')
    df.insert(0, 'id', pop_id)
    return df

def format_dates(df):
    """
    Formatea las fechas en el formato 'YYYY-MM-DD'
    
    Argumentos:
    df -- DataFrame de entrada
    
    Devuelve:
    df -- DataFrame con fechas formateadas en el formato 'YYYY-MM-DD'
    """   
    df['date_added'] = pd.to_datetime(df['date_added']).dt.strftime('%Y-%m-%d')
    return df

def lower_case(df):
    """Convierte todos los valores en las columnas de tipo 'object' de un DataFrame de Pandas a minúsculas.
    
    Argumentos:
    df(pandas.DataFrame): DataFrame a ser modificado.
    
    Devuelve:
    pandas.DataFrame: DataFrame con todos los valores en las columnas de tipo 'object' convertidos a minúsculas.
    """
    string_columns = [column for column in df.columns if df[column].dtype == 'object']
    df[string_columns] = df.loc[:,string_columns].apply(lambda x: x.str.lower())
    return df

def duration_distrib(df):
    """
    Divide la columna 'duration' en dos columnas 'duration_int' y 'duration_type'
    
    Argumentos:
    df -- DataFrame de entrada
    
    Devuelve:
    df -- DataFrame con dos columnas adicionales 'duration_int' y 'duration_type'
    """

    df[['duration_int', 'duration_type']] = df['duration'].str.split(expand=True)
    df['duration_int'] = df['duration_int'].astype(float).astype('Int64')
    df.drop(columns='duration', inplace=True)
    return df