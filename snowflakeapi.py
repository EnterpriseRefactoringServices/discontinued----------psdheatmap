import os.path
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas

class DatabaseApi:
    """
        This class provides a static interface to communicate with the database
        
    """
    connect_instance = None
    def __init__(self):
        """
            This instantiation does nothing.
            All the static methods of this class can be accessed from 
            the class or an instance of the class
        """
        pass

    @staticmethod
    def get_api_connection():
        """
            This static method returns a connection handle to the database
        """
        if DatabaseApi.connect_instance != None:
            return  DatabaseApi.connect_instance


        try:
            ctx = snowflake.connector.connect(
                user=os.environ['SNOWFLAKE_USER'],
                password=os.environ['SNOWFLAKE_PASSWORD'],
                account=os.environ['SNOWFLAKE_ACCOUNT'],
                database=os.environ['SNOWFLAKE_DATABASE'],
                schema=os.environ['SNOWFLAKE_SCHEMA'],
                warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
                role= os.environ['SNOWFLAKE_ROLE']
            )
            DatabaseApi.connect_instance = ctx 
            return ctx
        except Exception as e:
            print(f" Unable to connect to databse with error: {e} ")

    @staticmethod
    def destroy_api_connection():  
        ctx = DatabaseApi.get_api_connection()
        ctx.cursor().close()
        ctx.close()
        DatabaseApi.connect_instance = None

    @staticmethod
    def run_sql_query(sql_query:str):
        ctx = DatabaseApi.get_api_connection()
        cs = ctx.cursor()
        cs.execute(sql_query)
        rows = cs.fetchall()
        DatabaseApi.destroy_api_connection()
        return rows
                
           
