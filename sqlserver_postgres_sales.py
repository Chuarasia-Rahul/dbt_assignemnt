import dlt
from sqlalchemy import create_engine

# Define your tables
tables = ["currency", "currencyrate", "customer","salesorderdetail","salesorderheader","salesperson","salesterritory","store"]
schema = "sales"

engine = create_engine("mssql+pyodbc:///?odbc_connect="
                                    "DRIVER={ODBC Driver 17 for SQL Server};"
                                    "SERVER=OUTCAST-BATES;"
                                    "DATABASE=AdventureWorks2019;"
                                    "trusted_connection=yes")

for table in tables:
    with engine.connect() as conn:
        query = f"SELECT * FROM {schema}.{table}"
        rows = conn.execution_options(yield_per=100).exec_driver_sql(query)
        
        pipeline = dlt.pipeline(
            pipeline_name="from_database",
            destination="postgres",
            dataset_name="stg",
        )

        load_info = pipeline.run(rows, table_name=table, write_disposition="replace")

    print(load_info)