import os
import numpy as np
import pandas as pd

if __name__ == "__main__":
    cwd = os.getcwd()
    df = pd.read_csv("Data/indexData.csv", delimiter=',')

#CLEAN FILENAME SO NO ERRORS OCCUR WHEN IMPORTING INTO DATABASE
    file = "Data/indexData"
    clean_table_name = file.lower().replace(' ','_').replace('?','').replace('$','').replace('-','').replace('@','') \
        .replace('\\','').replace('(','').replace('/','').replace('%','').replace(')','').replace('+','') \
        .replace('=','')

#CLEAN COLUMN NAMES SO NO ERRORS OCCUR WHEN IMPORTING INTO DATABASE
    df.columns = [col.lower().replace(' ','_').replace('?','').replace('$','').replace('-','').replace('@','') \
        .replace('\\','').replace('(','').replace('/','').replace('%','').replace(')','').replace('+','') \
        .replace('=','') for col in df.columns]

#DICTIONARY TO REPLACE DATA TYPES OF PANDAS WITH DATA TYPES OF SQL
    replacements = {
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp',
        'timedelta64[ns]': 'varchar'
    }

    #print(df.dtypes)
    myTuple = ("John", "Peter", "Vicky")

    x = ", ".join("{} {} {}").format("John", "Peter", "Vicky")

    print(x)
