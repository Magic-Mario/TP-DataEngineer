import polars as pl
import numpy as np 
import pandas as pd

#* Cargar los archivos csv como dataframes

amazon_df = pl.read_csv('../Datasets/amazon_prime_titles-score.csv')
disney_plus_df = pl.read_csv('../Datasets/disney_plus_titles-score.csv')
netflix_title_df = pl.read_csv('../Datasets/netflix_titles-score.csv')

