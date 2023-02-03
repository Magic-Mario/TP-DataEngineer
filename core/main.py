#import polars as pl
#from Cleaning.cleaning import *
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



df_dict = {'amazon_df':amazon_df,
            'disney_plus':disney_plus_df,
            'netflix':netflix_title_df,
            'hulu':hulu_df}

app = FastAPI()


@app.get('/{df_name}', status_code=status.HTTP_200_OK)
async def get_dataframe(df_name: str):
    if df_name not in df_dict:
        return JSONResponse(content={"error": "DataFrame not found"})
    df = df_dict[df_name]
    return JSONResponse(content=df.to_json(orient='index'))




if __name__ == '__main__':    
    # * local Server using uvicorn
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)