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

path_input = args['BUCKET'] + "validated/" + file_name + ".parquet"
path_output = args['BUCKET'] + "refined/" + file_name + ".parquet"
data_df = wr.s3.read_parquet(path=path_input)
print(data_df.count())


if 'VEHICLE_ID' in data_df.columns:
   data_df["VEHICLE_ID"] = data_df.VEHICLE_ID.astype("string")

else:
   data_df["VEHICLE_ID"] = ""


if 'BAROMETRIC_PRESSURE_KPA_' in data_df.columns:
    print("BAROMETRIC_PRESSURE_KPA_ exists, renaming to BAROMETRIC_PRESSURE")
    data_df.rename(columns = {'BAROMETRIC_PRESSURE_KPA_':'BAROMETRIC_PRESSURE'}, inplace = True)

data_df["BAROMETRIC_PRESSURE"] = data_df["BAROMETRIC_PRESSURE"].astype("string").str.replace("kPa","")
data_df["BAROMETRIC_PRESSURE"] = data_df["BAROMETRIC_PRESSURE"].str.replace("NODATA","")
data_df = data_df.drop(data_df[data_df['BAROMETRIC_PRESSURE'].str.contains('E', case=False)].index)
data_df["BAROMETRIC_PRESSURE"] = data_df.BAROMETRIC_PRESSURE.str.replace(',', '.').astype(float)


data_df["ENGINE_COOLANT_TEMP"] = data_df["ENGINE_COOLANT_TEMP"].astype("string").str.replace("C","")
data_df["ENGINE_COOLANT_TEMP"] = data_df["ENGINE_COOLANT_TEMP"].str.replace("NODATA","")
data_df = data_df.drop(data_df[data_df['ENGINE_COOLANT_TEMP'].str.contains('E', case=False)].index)
data_df["ENGINE_COOLANT_TEMP"] = data_df.ENGINE_COOLANT_TEMP.str.replace(',', '.').astype(float)

data_df["FUEL_LEVEL"] = data_df["FUEL_LEVEL"].astype("string").str.replace("%","")
data_df["FUEL_LEVEL"] = data_df["FUEL_LEVEL"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['FUEL_LEVEL'].str.contains('E', case=False)].index)
data_df["FUEL_LEVEL"] = data_df.FUEL_LEVEL.str.replace(',', '.').astype(float)

data_df["ENGINE_LOAD"] = data_df["ENGINE_LOAD"].astype("string").str.replace("%","")
data_df["ENGINE_LOAD"] = data_df["ENGINE_LOAD"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['ENGINE_LOAD'].str.contains('E', case=False)].index)
data_df["ENGINE_LOAD"] = data_df.ENGINE_LOAD.str.replace(',', '.').astype(float)

data_df["AMBIENT_AIR_TEMP"] = data_df["AMBIENT_AIR_TEMP"].astype("string").str.replace("C","")
data_df["AMBIENT_AIR_TEMP"] = data_df["AMBIENT_AIR_TEMP"].str.replace("NODATA","0")
data_df["AMBIENT_AIR_TEMP"] = data_df["AMBIENT_AIR_TEMP"].str.replace("NaN","0")
data_df = data_df.drop(data_df[data_df['AMBIENT_AIR_TEMP'].str.contains('E', case=False)].index)
data_df["AMBIENT_AIR_TEMP"] = data_df.AMBIENT_AIR_TEMP.str.replace(',', '.').astype(float)

data_df["ENGINE_RPM"] = data_df["ENGINE_RPM"].astype("string").str.replace("RPM","")
data_df["ENGINE_RPM"] = data_df.ENGINE_RPM.str.replace(',', '.').astype(float)

data_df["INTAKE_MANIFOLD_PRESSURE"] = data_df["INTAKE_MANIFOLD_PRESSURE"].astype("string").str.replace("%","")
data_df["INTAKE_MANIFOLD_PRESSURE"] = data_df["INTAKE_MANIFOLD_PRESSURE"].astype("string").str.replace("kPa","")
data_df["INTAKE_MANIFOLD_PRESSURE"] = data_df["INTAKE_MANIFOLD_PRESSURE"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['INTAKE_MANIFOLD_PRESSURE'].str.contains('E', case=False)].index)
data_df["INTAKE_MANIFOLD_PRESSURE"] = data_df.INTAKE_MANIFOLD_PRESSURE.str.replace(',', '.').astype(float)


data_df["MAF"] = data_df["MAF"].astype("string").str.replace("g/s","")
data_df["MAF"] = data_df["MAF"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['MAF'].str.contains('E', case=False)].index)
data_df["MAF"] = data_df.MAF.str.replace(',', '.').astype(float)

data_df["FUEL_TYPE"] = data_df.MAF.astype("string")


data_df["AIR_INTAKE_TEMP"] = data_df["AIR_INTAKE_TEMP"].astype("string").str.replace("C","")
data_df = data_df.drop(data_df[data_df['AIR_INTAKE_TEMP'].str.contains('E', case=False)].index)
data_df["AIR_INTAKE_TEMP"] = data_df.AIR_INTAKE_TEMP.str.replace(',', '.').astype(float)

data_df["SPEED"] = data_df["SPEED"].astype("string").str.replace("km/h","")
data_df["SPEED"] = data_df["SPEED"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['SPEED'].str.contains('E', case=False)].index)
data_df["SPEED"] = data_df.SPEED.str.replace(',', '.').astype(float)


if 'Short_Term_Fuel_Trim_Bank_1' in data_df.columns:
    print("Short_Term_Fuel_Trim_Bank_1 exists, renaming to SHORT_TERM_FUEL_TRIM_BANK_1")
    data_df.rename(columns = {'Short_Term_Fuel_Trim_Bank_1':'SHORT_TERM_FUEL_TRIM_BANK_1'}, inplace = True)

data_df["SHORT_TERM_FUEL_TRIM_BANK_1"] = data_df["SHORT_TERM_FUEL_TRIM_BANK_1"].astype("string").str.replace("%","")
data_df["SHORT_TERM_FUEL_TRIM_BANK_1"] = data_df["SHORT_TERM_FUEL_TRIM_BANK_1"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['SHORT_TERM_FUEL_TRIM_BANK_1'].str.contains('E', case=False)].index)
data_df["SHORT_TERM_FUEL_TRIM_BANK_1"] = data_df["SHORT_TERM_FUEL_TRIM_BANK_1"].str.replace(',', '.').astype(float)


data_df["THROTTLE_POS"] = data_df["THROTTLE_POS"].astype("string").str.replace("%","")
data_df["THROTTLE_POS"] = data_df["THROTTLE_POS"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['THROTTLE_POS'].str.contains('E', case=False)].index)
data_df["THROTTLE_POS"] = data_df.THROTTLE_POS.str.replace(',', '.').astype(float)


data_df["TIMING_ADVANCE"] = data_df["TIMING_ADVANCE"].astype("string").str.replace("%","")
data_df["TIMING_ADVANCE"] = data_df["TIMING_ADVANCE"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['TIMING_ADVANCE'].str.contains('E', case=False)].index)
data_df["TIMING_ADVANCE"] = data_df.TIMING_ADVANCE.str.replace(',', '.').astype(float)

data_df["EQUIV_RATIO"] = data_df["EQUIV_RATIO"].astype("string").str.replace("%","")
data_df["EQUIV_RATIO"] = data_df["EQUIV_RATIO"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['EQUIV_RATIO'].str.contains('E', case=False)].index)
data_df["EQUIV_RATIO"] = data_df.EQUIV_RATIO.str.replace(',', '.').astype(float)


if 'Long_Term_Fuel_Trim_Bank_2' in data_df.columns:
    print("Long_Term_Fuel_Trim_Bank_2 exists, renaming to LONG_TERM_FUEL_TRIM_BANK_2")
    data_df.rename(columns = {'Long_Term_Fuel_Trim_Bank_2':'LONG_TERM_FUEL_TRIM_BANK_2'}, inplace = True)

if 'LONG_TERM_FUEL_TRIM_BANK_2' in data_df.columns:
    data_df["LONG_TERM_FUEL_TRIM_BANK_2"] = data_df["LONG_TERM_FUEL_TRIM_BANK_2"].astype("string").str.replace("%","")
    data_df["LONG_TERM_FUEL_TRIM_BANK_2"] = data_df["LONG_TERM_FUEL_TRIM_BANK_2"].str.replace("NODATA","0")
    data_df = data_df.drop(data_df[data_df['LONG_TERM_FUEL_TRIM_BANK_2'].str.contains('E', case=False)].index)
    data_df["LONG_TERM_FUEL_TRIM_BANK_2"] = data_df["LONG_TERM_FUEL_TRIM_BANK_2"].str.replace(',', '.').astype(float)
else:
    data_df["LONG_TERM_FUEL_TRIM_BANK_2"] = ""
    


if 'Short_Term_Fuel_Trim_Bank_2' in data_df.columns:
    print("Short_Term_Fuel_Trim_Bank_2 exists, renaming to SHORT_TERM_FUEL_TRIM_BANK_2")
    data_df.rename(columns = {'Short_Term_Fuel_Trim_Bank_2':'SHORT_TERM_FUEL_TRIM_BANK_2'}, inplace = True)

if 'SHORT_TERM_FUEL_TRIM_BANK_2' in data_df.columns:
    data_df["SHORT_TERM_FUEL_TRIM_BANK_2"] = data_df["SHORT_TERM_FUEL_TRIM_BANK_2"].astype("string").str.replace("%","")
    data_df["SHORT_TERM_FUEL_TRIM_BANK_2"] = data_df["SHORT_TERM_FUEL_TRIM_BANK_2"].str.replace("NODATA","0")
    data_df = data_df.drop(data_df[data_df['SHORT_TERM_FUEL_TRIM_BANK_2'].str.contains('E', case=False)].index)
    data_df["SHORT_TERM_FUEL_TRIM_BANK_2"] = data_df["SHORT_TERM_FUEL_TRIM_BANK_2"].str.replace(',', '.').astype(float)
else:
    data_df["SHORT_TERM_FUEL_TRIM_BANK_2"] = ""
   


data_df["FUEL_PRESSURE"] = data_df["FUEL_PRESSURE"].astype("string").str.replace("kPa","")
data_df["FUEL_PRESSURE"] = data_df["FUEL_PRESSURE"].str.replace("NODATA","0")
data_df = data_df.drop(data_df[data_df['FUEL_PRESSURE'].str.contains('E', case=False)].index)
data_df["FUEL_PRESSURE"] = data_df.FUEL_PRESSURE.str.replace(',', '.').astype(float)



data_df=data_df[['VEHICLE_ID','BAROMETRIC_PRESSURE','ENGINE_COOLANT_TEMP','FUEL_LEVEL','ENGINE_LOAD','AMBIENT_AIR_TEMP','ENGINE_RPM',
      'INTAKE_MANIFOLD_PRESSURE','MAF', 'LONG_TERM_FUEL_TRIM_BANK_2','FUEL_TYPE', 'AIR_INTAKE_TEMP', 'FUEL_PRESSURE', 
      'SPEED','SHORT_TERM_FUEL_TRIM_BANK_2', 'SHORT_TERM_FUEL_TRIM_BANK_1',  'ENGINE_RUNTIME','THROTTLE_POS','DTC_NUMBER',
      'TROUBLE_CODES','TIMING_ADVANCE','EQUIV_RATIO']]

data_df['date_uploaded'] = file_name[4:14].replace("_","-") + " " + file_name[15:23].replace("_",":")
data_df['date_uploaded'] = pd.to_datetime(data_df['date_uploaded'], format="%d-%m-%Y %H:%M:%S")
print(data_df.dtypes)
print(data_df.count())

insert_data(data_df, path_output)