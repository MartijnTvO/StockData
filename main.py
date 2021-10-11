import numpy as np
import pandas as pd
import pypyodbc

if __name__ == "__main__":
#SET GLOBAL VARIABLES
    f = open("Info.txt", "r")
    FILENAME, SERVER_NAME, DATABASE_NAME = f.read().split('\n')
    FILENAME = FILENAME.split('=')[1]
    SERVER_NAME = SERVER_NAME.split('=')[1]
    DATABASE_NAME = DATABASE_NAME.split('=')[1]

#READ FILE AND INSERT INTO DATAFRAME
    df = pd.read_csv("Data/"+FILENAME, delimiter=',')

#CLEAN FILENAME SO NO ERRORS OCCUR WHEN IMPORTING INTO DATABASE
    file = FILENAME.split('.')[0]
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

#OPEN SERVER CONNECTION
    conn = pypyodbc.connect("""
                Driver={{ODBC Driver 17 for SQL Server}};
                Server={0};
                Trusted_Connection=yes;""".format(SERVER_NAME), autocommit=False
                            )
    cursor = conn.cursor()

#CREATES DATABASE IF NOT EXISTS AND CONNECTS TO IT
    query = """
                IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = ?) 
                BEGIN
                CREATE DATABASE ?
                END  
            """
    params = (DATABASE_NAME, DATABASE_NAME)
    cursor.execute(query, params)
    conn.commit()

    conn = pypyodbc.connect("""
                Driver={{ODBC Driver 17 for SQL Server}};
                Server={0};
                Database={1};
                Trusted_Connection=yes;""".format(SERVER_NAME, DATABASE_NAME), autocommit=False
                            )
    cursor = conn.cursor()

#DROP TABLE WITH SAME NAME
    cursor.execute("""
            IF OBJECT_ID(N'dbo.[{0}]', N'U') IS NOT NULL  
            DROP TABLE [dbo].[{0}];  
        """.format(file))

#CREATE TABLE
    cursor.execute("""
            CREATE TABLE {0} ({1})
        """.format(file, col_str))

#SAVE DF TO CSV (df.to_csv('indexData.csv', header=df.columns, index=False, encoding=utf-8')


#OPEN THE CSV FILE, SAVE IT AS OBJECT, UPLOAD TO DB
