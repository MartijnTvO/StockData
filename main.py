import os
import numpy as np
import pandas as pd
import pypyodbc

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

#DATA SCIENCE STUFF


#DICTIONARY TO REPLACE DATA TYPES OF PANDAS WITH DATA TYPES OF SQL
    replacements = {
        'object': 'varchar(50)',
        'float64': 'DECIMAL(8,2)',
        'int64': 'int',
        'datetime64': 'timestamp',
        'timedelta64[ns]': 'varchar'
    }

    col_str = ", ".join("\"{}\" {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))

#OPEN DATABASE CONNECTION
    SERVER_NAME = "DESKTOP-MARTIJN"
    DATABASE_NAME = "StockData"
    TABLE_NAME = file.split('/')[1]
    conn = pypyodbc.connect("""
                Driver={{ODBC Driver 17 for SQL Server}};
                Server={0};
                Trusted_Connection=yes;""".format(SERVER_NAME)
                            )
    cursor = conn.cursor()

#CREATES DATABASE IF NOT EXISTS AND CONNECTS TO IT
    cursor.execute("""
                IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{0}') 
                BEGIN
                CREATE DATABASE [{0}]
                END  
            """.format(DATABASE_NAME))

    conn = pypyodbc.connect("""
                Driver={{ODBC Driver 17 for SQL Server}};
                Server={0};
                Database={1};
                Trusted_Connection=yes;""".format(SERVER_NAME, DATABASE_NAME)
                            )
    cursor = conn.cursor()

#DROP TABLE WITH SAME NAME
    cursor.execute("""
            IF OBJECT_ID(N'dbo.[{0}]', N'U') IS NOT NULL  
            DROP TABLE [dbo].[{0}];  
        """.format(TABLE_NAME))

#CREATE TABLE
    cursor.execute("""
            CREATE TABLE {0} ({1})
        """.format(TABLE_NAME, col_str))

#SAVE DF TO CSV (df.to_csv('indexData.csv', header=df.columns, index=False, encoding=utf-8')


#OPEN THE CSV FILE, SAVE IT AS OBJECT, UPLOAD TO DB