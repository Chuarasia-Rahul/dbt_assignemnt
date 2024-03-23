import dlt
from sqlalchemy import create_engine
from dlt.destinations import postgres

# Define your tables
tables1 = ["actor", "address", "category"]
schema1 = "stg"
tables2 =  ["dim_customer", "dim_Date"]
schema2 = "shd"
tables3 =  ["family"]
schema3 = "rfam"

engine = create_engine("postgresql://loader:Rahul_1234@localhost:5432/sakila_wh")
engine2 = create_engine("postgresql://loader:Rahul_1234@localhost:5432/postgres")

for table in tables1:
    with engine.connect() as conn:
        query = f"SELECT * FROM {schema1}.{table}"
        rows = conn.exec_driver_sql(query) #conn.execution_options(yield_per=100).exec_driver_sql(query)
        
        pipeline = dlt.pipeline(
            pipeline_name="from_schema",
            destination=postgres,
            dataset_name="dlt_pipeline",
        )

        load_info = pipeline.run(rows, table_name=table + "_stg", write_disposition="replace")

    print(load_info)
    
for table in tables2:
    with engine.connect() as conn:
        query = f"SELECT * FROM {schema2}.{table}"
        rows = conn.execution_options(yield_per=100).exec_driver_sql(query)
        
        pipeline = dlt.pipeline(
            pipeline_name="from_schema",
            destination=postgres,
            dataset_name="dlt_pipeline"
        )

        load_info = pipeline.run(rows, table_name=table + "_shd", write_disposition="replace")

    print(load_info)
    
for table in tables3:
    with engine2.connect() as conn:
        query = f"SELECT * FROM {schema3}.{table}"
        rows = conn.exec_driver_sql(query) #conn.execution_options(yield_per=100).exec_driver_sql(query)
        
        pipeline = dlt.pipeline(
            pipeline_name="from_schema",
            destination=postgres,
            dataset_name="dlt_pipeline",
        )

        load_info = pipeline.run(rows, table_name=table + "_rfam", write_disposition="replace")

    print(load_info)