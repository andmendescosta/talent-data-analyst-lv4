import requests
import os
import glob
import gzip
import shutil
import pandas as pd
from dateutil import parser

def get_document_type(document):
    if len(document) == 11:
        return 'CPF'
    elif len(document) == 14:
        return 'CNPJ'
    else:
        return 'N/A'

def get_normalized_date(date):
    parsed_date = parser.parse(date)
    return parsed_date.strftime("%Y-%m-%d")

# Download File
URL = "https://st-it-cloud-public.s3.amazonaws.com/people-v2_1E6.csv.gz"
response = requests.get(URL)
open("./tmp/people-v2_1E6.csv.gz", "wb").write(response.content)

#Decompress File
with gzip.open('./tmp/people-v2_1E6.csv.gz', 'rb') as entrada:
    with open('./tmp/people-v2_1E6.csv', 'wb') as saida:
        shutil.copyfileobj(entrada, saida)

# Read File
df = pd.read_csv ('./tmp/people-v2_1E6.csv', sep=';')

df['document'] = df['document'].str.replace('\W', '', regex=True)
df['document_type'] = df['document'].apply(lambda x: get_document_type(x))

df['birthDate'] = df['birthDate'].apply(lambda x: get_normalized_date(x))


df.to_csv('./output/exercicio2.csv', sep=';', encoding='utf-8')
df.to_parquet('./output/exercicio2.parquet', engine='fastparquet')

# Remove Files
files = glob.glob('./tmp/*')
for f in files:
    os.remove(f)