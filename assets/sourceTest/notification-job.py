import sys
import pandas as pd
import awswrangler as wr
import boto3
from awsglue.utils import getResolvedOptions


client = boto3.client('sns')

args = getResolvedOptions(sys.argv, ['KEY', 'BUCKET', 'SNS_ARN'])
key_value = args['KEY']
sns_arn = args['SNS_ARN']
file_name = key_value.split('/')[-1].split('.')[0]

path_input = args['BUCKET'] + "loaded/" + file_name + ".parquet"

data_df = wr.s3.read_parquet(path=path_input)

#CONDICIONES DE ENVÍO
#Notificar en caso existan códigos de error
data_df["TROUBLE_CODES"] = data_df["TROUBLE_CODES"].fillna("NOCODE")

send_code = []
for i in data_df["TROUBLE_CODES"].unique():
    if i != "NOCODE":
        send_code.append(i)

if len(send_code) > 0:
    print(send_code)
    response = client.publish(
        TopicArn = sns_arn,
        Message = ' '.join(map(str,send_code)),
        Subject = 'Trouble codes found'
    )
    print(response)
