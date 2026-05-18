from google.cloud import bigquery
import pandas as pd



# param url

# fonction extraction csv

def extraction_csv(url):
    data_read_csv=pd.read_csv(url)
    return data_read_csv


__