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

path_input = args['BUCKET'] + "refined/" + file_name + ".parquet"
path_output = args['BUCKET'] + "aggregated/" + file_name + ".parquet"
path_code_dict = args['BUCKET'] + "otras-fuentes/code_dict.parquet"

data_df = wr.s3.read_parquet(path=path_input)
data_df["MI_TROUBLE_CODE"] = data_df["TROUBLE_CODES"].astype("string").str.slice(0, 5)

code_dict_df = wr.s3.read_parquet(path=path_code_dict)
print(code_dict_df)


data_df = data_df.merge(code_dict_df,left_on='MI_TROUBLE_CODE', right_on='codes', how='left')

print(data_df.dtypes)

insert_data(data_df, path_output)