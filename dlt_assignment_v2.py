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

engine = create_engine("postgresql://loader:Rahul_1234@localhost:5432/sakila_wh")
engine2 = create_engine("postgresql://loader:Rahul_1234@localhost:5432/postgres")

for schema,tables in tables1.items():
    for table in tables:
        with engine.connect() as conn:
            query = f"SELECT * FROM {schema}.{table}"
            rows = conn.exec_driver_sql(query)          #conn.execution_options(yield_per=100).exec_driver_sql(query)
        
        pipeline = dlt.pipeline(
            pipeline_name="from_schema",
            destination=postgres,
            dataset_name="dlt_pipeline",
        )

        load_info = pipeline.run(rows, table_name=table + "_stg", write_disposition="replace")

    print(load_info)
    
    
for schema,tables in tables2.items():
    for table in tables:
        with engine2.connect() as conn:
            query = f"SELECT * FROM {schema}.{table}"
            rows = conn.exec_driver_sql(query) #conn.execution_options(yield_per=100).exec_driver_sql(query)
        
        pipeline = dlt.pipeline(
            pipeline_name="from_schema",
            destination=postgres,
            dataset_name="dlt_pipeline",
        )

        load_info = pipeline.run(rows, table_name=table + "_rfam", write_disposition="replace")

    print(load_info)