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
        wr.s3.to_parquet(write_params)
        print(f'Data escrita en {path}')
    except Exception as e:
        raise Exception(f"Error al insertar datos en {path}, error: {str(e)}")


args = getResolvedOptions(sys.argv, ['KEY', 'BUCKET'])
key_value = args['KEY']
file_name = key_value.split('/')[-1].split('.')[0]

path_input = args['BUCKET'] + "aggregated/" + file_name + ".parquet"
path_output = args['BUCKET'] + "loaded/" + file_name + ".parquet"

data_df = wr.s3.read_parquet(path=path_input)

print(data_df)

insert_data(data_df, path_output)

