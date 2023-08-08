import logging
import sys
import pandas as pd
import awswrangler as wr
from awsglue.utils import getResolvedOptions


def insert_data(data, path):
    try:
        write_params = {
            "df": data,
            "path": path,
            "compression": 'snappy'
        }
        wr.s3.to_parquet(**write_params)
        print(f'Data escrita en {path}')
    except Exception as e:
        raise Exception(f"Insertando datos en {path} error: {str(e)}")


args = getResolvedOptions(sys.argv, ['KEY', 'BUCKET'])
key_value = args['KEY']
file_name = key_value.split('/')[-1].split('.')[0]
path_input = args['BUCKET'] + "charged/" + file_name + ".csv"
path_output = args['BUCKET'] + "validated/" + file_name + ".parquet"

if key_value[-3:] != "csv":
    raise ValueError('El archivo cargado no esta en el formato csv.')


dim = wr.s3.read_csv(path=path_input,
                     sep=';',
                     skiprows=1)

if dim.shape[0] == 0:
    raise ValueError('El archivo cargado no tiene valores.')
  
print(dim)
insert_data(dim, path_output)