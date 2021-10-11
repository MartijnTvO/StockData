import numpy as np
import pandas as pd
import pyodbc

if __name__ == "__main__":
#SET GLOBAL VARIABLES
    f = open("Info.txt", "r")
    FILENAME, SERVER_NAME, DATABASE_NAME = f.read().split('\n')
    FILENAME = FILENAME.split('=')[1]
    SERVER_NAME = SERVER_NAME.split('=')[1]
    DATABASE_NAME = DATABASE_NAME.split('=')[1]

#READ FILE AND INSERT INTO DATAFRAME
    df = pd.read_csv("Data/"+FILENAME, delimiter=',')

#CLEANING THE DATA AND ADDING COLUMNS
    print(df.head())
    df['Intraday Change'] = df['Adj Close'] - df['Open']
    df['Daily P/L'] = df['Adj Close'] - df['Adj Close'].shift(1)
    df['TenDay MA'] = df['Adj Close'].rolling(window=10).mean()
    df['TwentyDay MA'] = df['Adj Close'].rolling(window=20).mean()
    df['FiftyDay MA'] = df['Adj Close'].rolling(window=50).mean()

#CLEAN FILENAME SO NO ERRORS OCCUR WHEN IMPORTING INTO DATABASE
    file = FILENAME.split('.')[0]
    clean_table_name = file.lower().replace(' ','_').replace('?','').replace('$','').replace('-','').replace('@','') \
        .replace('\\','').replace('(','').replace('/','').replace('%','').replace(')','').replace('+','') \
        .replace('=','')

#CLEAN COLUMN NAMES SO NO ERRORS OCCUR WHEN IMPORTING INTO DATABASE
    df.columns = [col.lower().replace(' ','_').replace('?','').replace('$','').replace('-','').replace('@','') \
        .replace('\\','').replace('(','').replace('/','').replace('%','').replace(')','').replace('+','') \
        .replace('=','') for col in df.columns]

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
    try:
        conn = pyodbc.connect("""
                    Driver={{SQL Server}};
                    Server={0};
                    Database={1};
                    Trusted_Connection=yes;""".format(SERVER_NAME, DATABASE_NAME), autocommit=False
                              )
    except Exception as e:
        print(e)
        print("Could not connect")
    else:
        cursor = conn.cursor()

#DROP TABLE WITH SAME NAME
    try:
        cursor.execute("""
                IF OBJECT_ID(N'dbo.[{0}]', N'U') IS NOT NULL
                DROP TABLE [dbo].[{0}];
            """.format(file))
    except Exception as e:
        print(e)
        print("Error dropping table")
    else:
        cursor.commit()

#CREATE TABLE
    try:
        cursor.execute("""
                CREATE TABLE {0} ({1})
            """.format(file, col_str))
    except Exception as e:
        print(e)
        print("Error creating table")
    else:
        cursor.commit()

#OPEN THE CSV FILE, SAVE IT AS OBJECT, UPLOAD TO DB
    insert_statement = """
            INSERT INTO {0}
            VALUES {1}
        """


    try:
        df_header = df.iloc[0]
        df = df[1]
        df.columns = df_header
        for index in df.index:
            print(df.iloc[[index]])
            cursor.execute(insert_statement.format(file, df.iloc[[index]]))
    except Exception as e:
        cursor.rollback()
        print(e)
        print('transaction rolled back')
    else:
        print('records inserted successfully')
        cursor.commit()
        cursor.close()
    finally:
        if conn == True:
            print('connection closed')
            conn.close()
