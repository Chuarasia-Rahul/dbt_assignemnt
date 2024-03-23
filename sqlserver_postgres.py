import dlt
from dlt.sources.sql_database import table

# Define the source (SQL Server)
def sql_server_source():
    source = table(
        credentials="mssql+pyodbc://@OUTCAST-BATES/AdventureWorks2019?trusted_connection=yes&driver=DRIVER",  # replace with your connection string
        table="production.product",  # replace with your table name
    )
    return source

# Define the pipeline
def load_data():
    pipeline = dlt.pipeline(
        pipeline_name='sql_to_postgres', 
        destination='postgres', 
        dataset_name='sql_data', 
        credentials="postgresql://loader:Rahul_1234@localhost/dlt_data"  # replace with your credentials
    )
    source = sql_server_source()
    print(pipeline.run(source))

if __name__ == "__main__":
    load_data()