
import numpy as np
import pandas as pd
from fastapi import FastAPI, status
from fastapi.responses import Response, JSONResponse
from fastapi.exceptions import HTTPException
from fastapi.params import Body
from pydantic import BaseModel

#* Cargar los archivos csv como dataframes
amazon_df = pd.read_csv('Datasets/amazon_prime_titles-score.csv')
disney_plus_df = pd.read_csv('Datasets/disney_plus_titles-score.csv')
netflix_title_df = pd.read_csv('Datasets/netflix_titles-score.csv')
hulu_df = pd.read_csv('Datasets/hulu_titles-score (2).csv')


"""CLEANING THE DATA"""

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

def count_keywords(df, keyword:str):
    return df['title'].apply(lambda x: x.lower().count(keyword)).sum()

def movie_rating(df, score:int, year:int):
    return df[(df['score'] > score) & (df['release_year'] == year)]

df_dict = {'amazon':amazon_df,
            'disney_plus':disney_plus_df,
            'netflix':netflix_title_df,
            'hulu':hulu_df}
def second_max_scoring(df):
    return df.sort_values(by='title')['title'].iloc[1]

def high_duration_movie(df):
    if df[df['duration_type'] == 'seasons']:
        return df.sort_values(by=['release_year', 'duration_type'])['title'].iloc[0]
    


#* --------------------------------- MAIN FUNC ------------------------------------------------------------------




def main():
    
    #* Applying func fill_na
    fill_na(amazon_df)
    fill_na(disney_plus_df)
    fill_na(netflix_title_df)
    fill_na(hulu_df)
    
    #* Applying func create_id_column and generate_id
    create_id_column(amazon_df)
    create_id_column(disney_plus_df)
    create_id_column(hulu_df)
    create_id_column(netflix_title_df)
    
    #* Applying func format_dates
    format_dates(amazon_df)
    format_dates(hulu_df)
    format_dates(disney_plus_df)
    format_dates(netflix_title_df)
    
    #* Applying func lower_case
    lower_case(amazon_df)
    lower_case(hulu_df)
    lower_case(disney_plus_df)
    lower_case(netflix_title_df)
    
    #* Applying func duration_distrib
    duration_distrib(amazon_df)
    duration_distrib(disney_plus_df)
    duration_distrib(hulu_df)
    duration_distrib(netflix_title_df)

#* --------------------------------- WORKING WITH API -----------------------------------------------------------

import json

app = FastAPI()


@app.get('full-df/{df_name}', status_code=status.HTTP_200_OK)
async def get_dataframe(df_name: str):
    """
    Endpoint que permite obtener un DataFrame específico a partir de su nombre.

    :param df_name: Nombre del DataFrame que se desea obtener.
    :return: JSONResponse con el contenido del DataFrame en formato JSON.
    :raise HTTPException: Si el nombre del DataFrame no se encuentra en el diccionario.
    """
    if df_name not in df_dict:
        raise HTTPException(status_code=404, detail="DataFrame not found")
    df = df_dict[df_name]
    return JSONResponse(content=df.to_json(orient='index'))


@app.get("full-df/{df_name}/{row_id}", status_code=status.HTTP_200_OK)
def get_row(df_name: str, row_id: int):
    """
    Endpoint que permite obtener una fila específica de un DataFrame a partir de su nombre y ID de fila.

    :param df_name: Nombre del DataFrame que se desea obtener.
    :param row_id: ID de la fila que se desea obtener.
    :return: JSONResponse con la fila en formato JSON.
    :raise HTTPException: Si el nombre del DataFrame no se encuentra en el diccionario o si el ID de fila es inválido.
    """
    if df_name not in df_dict:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='DataFrame not found')
    df = df_dict[df_name]
    if row_id > len(df) or row_id < 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Row not found')
    row = df.iloc[row_id]
    return JSONResponse(content=json.dumps(row.to_dict()))



if __name__ == '__main__':
    #* Used to apply all changes to the dataframes
    main()    
    # * local Server using uvicorn
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)