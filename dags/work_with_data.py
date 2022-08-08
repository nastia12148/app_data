import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def _creating_streams():
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn").get_conn()
    cur = dwh_hook.cursor()

    sql = '''CREATE TABLE RAW_TABLE(IOS_App_Id NUMBER, 
    Title VARCHAR,
	Developer_Name VARCHAR,
	Developer_IOS_Id FLOAT,
	IOS_Store_Url VARCHAR,
	Seller_Official_Website VARCHAR,
	Age_Rating VARCHAR,
	Total_Average_Rating FLOAT,
	Total_Number_of_Ratings FLOAT,
	Average_Rating_For_Version FLOAT,
	Number_of_Ratings_For_Version NUMBER,
	Original_Release_Date VARCHAR,
	Current_Version_Release_Date VARCHAR,
	Price_USD FLOAT,
	Primary_Genre VARCHAR,
	All_Genres VARCHAR,
	Languages VARCHAR,
	Description VARCHAR
    )'''
    cur.execute(sql)

    sql = '''CREATE TABLE STAGE_TABLE(IOS_App_Id NUMBER, 
        Title VARCHAR,
    	Developer_Name VARCHAR,
    	Developer_IOS_Id FLOAT,
    	IOS_Store_Url VARCHAR,
    	Seller_Official_Website VARCHAR,
    	Age_Rating VARCHAR,
    	Total_Average_Rating FLOAT,
    	Total_Number_of_Ratings FLOAT,
    	Average_Rating_For_Version FLOAT,
    	Number_of_Ratings_For_Version NUMBER,
    	Original_Release_Date VARCHAR,
    	Current_Version_Release_Date VARCHAR,
    	Price_USD FLOAT,
    	Primary_Genre VARCHAR,
    	All_Genres VARCHAR,
    	Languages VARCHAR,
    	Description VARCHAR
        )'''
    cur.execute(sql)

    sql = '''CREATE TABLE MASTER_TABLE(IOS_App_Id NUMBER, 
        Title VARCHAR,
    	Developer_Name VARCHAR,
    	Developer_IOS_Id FLOAT,
    	IOS_Store_Url VARCHAR,
    	Seller_Official_Website VARCHAR,
    	Age_Rating VARCHAR,
    	Total_Average_Rating FLOAT,
    	Total_Number_of_Ratings FLOAT,
    	Average_Rating_For_Version FLOAT,
    	Number_of_Ratings_For_Version NUMBER,
    	Original_Release_Date VARCHAR,
    	Current_Version_Release_Date VARCHAR,
    	Price_USD FLOAT,
    	Primary_Genre VARCHAR,
    	All_Genres VARCHAR,
    	Languages VARCHAR,
    	Description VARCHAR
        )'''
    cur.execute(sql)

    sql = "CREATE OR REPLACE STREAM RAW_STREAM ON TABLE RAW_TABLE;"
    cur.execute(sql)

    sql = "CREATE OR REPLACE STREAM STAGE_STREAM ON TABLE STAGE_TABLE;"
    cur.execute(sql)

    cur.close()
    dwh_hook.close()


def _load_to_snowflake():
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn").get_conn()

    original = r"/home/nastia12148/PycharmProjects/airflow/airflow/data/763K_plus_IOS_Apps_Info.csv"
    delimiter = ","
    total = pd.read_csv(original, sep=delimiter, names=['_ID', 'IOS_APP_ID', 'TITLE', 'DEVELOPER_NAME',
                                                        'DEVELOPER_IOS_ID',
                                                        'IOS_STORE_URL',
                                                        'SELLER_OFFICIAL_WEBSITE',
                                                        'AGE_RATING',
                                                        'TOTAL_AVERAGE_RATING',
                                                        'TOTAL_NUMBER_OF_RATINGS',
                                                        'AVERAGE_RATING_FOR_VERSION',
                                                        'NUMBER_OF_RATINGS_FOR_VERSION',
                                                        'ORIGINAL_RELEASE_DATE',
                                                        'CURRENT_VERSION_RELEASE_DATE',
                                                        'PRICE_USD',
                                                        'PRIMARY_GENRE',
                                                        'ALL_GENRES',
                                                        'LANGUAGES',
                                                        'DESCRIPTION'], nrows=100)

    total = total.drop(["_ID"], axis=1)
    total = total.drop(0, axis=0)

    nul: object = 0

    values = {'IOS_APP_ID': nul,
              'TITLE': "-",
              'DEVELOPER_NAME': "-",
              'DEVELOPER_IOS_ID': nul,
              'IOS_STORE_URL': "-",
              'SELLER_OFFICIAL_WEBSITE': "-",
              'AGE_RATING': "-",
              'TOTAL_AVERAGE_RATING': nul,
              'TOTAL_NUMBER_OF_RATINGS': nul,
              'AVERAGE_RATING_FOR_VERSION': nul,
              'NUMBER_OF_RATINGS_FOR_VERSION': nul,
              'ORIGINAL_RELEASE_DATE': "-",
              'CURRENT_VERSION_RELEASE_DATE': "-",
              'PRICE_USD': nul,
              'PRIMARY_GENRE': "-",
              'ALL_GENRES': "-",
              'LANGUAGES': "-",
              'DESCRIPTION': "-"}
    total.fillna(value=values)

    total["IOS_APP_ID"] = pd.to_numeric(total["IOS_APP_ID"])
    total["DEVELOPER_IOS_ID"] = pd.to_numeric(total["DEVELOPER_IOS_ID"])
    total["TOTAL_AVERAGE_RATING"] = pd.to_numeric(total["TOTAL_AVERAGE_RATING"])
    total["TOTAL_NUMBER_OF_RATINGS"] = pd.to_numeric(total["TOTAL_NUMBER_OF_RATINGS"])
    total["AVERAGE_RATING_FOR_VERSION"] = pd.to_numeric(total["AVERAGE_RATING_FOR_VERSION"])
    total["NUMBER_OF_RATINGS_FOR_VERSION"] = pd.to_numeric(total["NUMBER_OF_RATINGS_FOR_VERSION"])
    total["PRICE_USD"] = pd.to_numeric(total["PRICE_USD"])

    write_pandas(dwh_hook, total, "RAW_TABLE")

    dwh_hook.close()


def _from_raw_to_stage():
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn").get_conn()
    cur = dwh_hook.cursor()

    sql = '''insert into stage_table
select IOS_APP_ID, TITLE, DEVELOPER_NAME, DEVELOPER_IOS_ID, IOS_STORE_URL,SELLER_OFFICIAL_WEBSITE,AGE_RATING,
TOTAL_AVERAGE_RATING,TOTAL_NUMBER_OF_RATINGS,AVERAGE_RATING_FOR_VERSION,NUMBER_OF_RATINGS_FOR_VERSION,
ORIGINAL_RELEASE_DATE,CURRENT_VERSION_RELEASE_DATE,PRICE_USD,PRIMARY_GENRE,ALL_GENRES,LANGUAGES,DESCRIPTION 
from raw_stream;'''
    cur.execute(sql)

    cur.close()
    dwh_hook.close()


def _from_stage_to_master():
    dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn").get_conn()
    cur = dwh_hook.cursor()

    sql = '''insert into master_table
select IOS_APP_ID, TITLE, DEVELOPER_NAME, DEVELOPER_IOS_ID, IOS_STORE_URL,SELLER_OFFICIAL_WEBSITE,AGE_RATING,
TOTAL_AVERAGE_RATING,TOTAL_NUMBER_OF_RATINGS,AVERAGE_RATING_FOR_VERSION,NUMBER_OF_RATINGS_FOR_VERSION,
ORIGINAL_RELEASE_DATE,CURRENT_VERSION_RELEASE_DATE,PRICE_USD,PRIMARY_GENRE,ALL_GENRES,LANGUAGES,DESCRIPTION 
from stage_stream;'''

    cur.execute(sql)

    cur.close()
    dwh_hook.close()
