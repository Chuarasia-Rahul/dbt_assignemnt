import dlt
from sqlalchemy import create_engine
from dlt.destinations import postgres

# Define your tables
tables1 = {
    "stg": ["actor", "address", "category"],
    "shd": ["dim_customer", "dim_Date"],
}

tables2 = {
    "rfam": ["family"],
}

engine_config = {
    "sakila_wh": "postgresql://loader:Rahul_1234@localhost:5432/sakila_wh",
    "postgres": "postgresql://loader:Rahul_1234@localhost:5432/postgres"
}

def execute_pipeline(engine, schema, tables):
    with engine.connect() as conn:
        for table in tables:
            query = f"SELECT * FROM {schema}.{table}"
            rows = conn.exec_driver_sql(query)

            pipeline = dlt.pipeline(
                pipeline_name="from_schema",
                destination=postgres,
                dataset_name="dlt_pipeline",
            )

            load_info = pipeline.run(rows, table_name=f"{table}_{schema}", write_disposition="replace")
            print(load_info)

for schema, tables in tables1.items():
    execute_pipeline(create_engine(engine_config["sakila_wh"]), schema, tables)

for schema, tables in tables2.items():
    execute_pipeline(create_engine(engine_config["postgres"]), schema, tables)
