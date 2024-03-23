import dlt
from sqlalchemy import create_engine
from dlt.destinations import postgres
import toml

# Read database connection details from TOML file
db_config = toml.load("database_config.toml")

# Define your tables
tables = {
    "sakila_wh.stg": ["actor", "address", "category"],
    "sakila_wh.shd": ["dim_customer", "dim_Date"],
    "postgres.rfam": ["dim_customer", "dim_Date"],
    "adventure_db.stg": ["product", "productcategory"],
}

# Function to execute pipeline for each table
def execute_pipeline(engine, schema, table_name):
    with engine.connect() as conn:
        query = f"SELECT * FROM {schema}.{table_name}"
        result = conn.exec_driver_sql(query)  # Use conn.execute() to execute the query
        rows = result.fetchall()  # Fetch all rows from the result
        
        pipeline = dlt.pipeline(
            pipeline_name="from_schema",
            destination=postgres,
            dataset_name="dlt_pipeline"
        )

        load_info = pipeline.run(rows, table_name=f"{table_name}_{schema}", write_disposition="replace")

    print(load_info)

# Establish database connections and execute pipelines
for schema, schema_tables in tables.items():
    for table in schema_tables:
        # Get connection details for the schema
        conn_details = db_config.get(schema)
        if conn_details:
            # Assuming connection details contain a key called "connection_string"
            engine = create_engine(conn_details["connection_string"])
            execute_pipeline(engine, schema, table)
        else:
            print(f"No connection details found for schema '{schema}'")

