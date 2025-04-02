import great_expectations as gx

from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

import yaml

## Not managed to get it working for SQL Server instance - various attempts below - tried their doc on MSSQL - doesnt work
# This does not work:  https://legacy.017.docs.greatexpectations.io/docs/0.14.13/guides/connecting_to_your_data/database/mssql/ 


# from sqlalchemy import create_engine, engine

context = gx.get_context()


datasource_yaml = r"""
name: my_mssql_datasource
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: mssql+pyodbc://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?driver=<DRIVER>&charset=utf&autocommit=true
data_connectors:
   default_runtime_data_connector_name:
       class_name: RuntimeDataConnector
       batch_identifiers:
           - default_identifier_name
   default_inferred_data_connector_name:
       class_name: InferredAssetSqlDataConnector
       include_schema_name: true
"""


datasource_config = {
    "name": "my_mssql_datasource",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": "mssql+pyodbc://sa:Badger99@127.0.0.1:1007/securities_masterC2?driver='ODBC Driver 18 for SQL Server'&charset=utf&autocommit=true",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "include_schema_name": True,
        },
    },
}



from great_expectations.datasource.fluent import SQLDatasource


my_datasource = SQLDatasource(
    name="my_mssql_datasource",
    connection_string="mssql+pyodbc://sa:Badger99@127.0.0.1:1007/securities_masterC2?driver=ODBC+Driver+18+for+SQL+Server&charset=utf&autocommit=true"
    # connection_string="Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1,1007;Database=securities_masterC2;uid=sa;pwd=Badger99;TrustServerCertificate=yes"  
)


context.data_sources.add_sql(my_datasource)


context.sources.add_sql(name="my_mssql_datasource", connection_string=my_datasource.connection_string)

# context.add_datasource(my_datasource)



# context.add_datasource(**yaml.load(datasource_yaml))

context.add_datasource(**datasource_config)





# configFile = open("config.yaml")
# configDict = yaml.load(configFile, Loader=yaml.FullLoader)


# engine_url = engine.URL.create(
#         drivername='mssql',
#         username=configDict['database']['username'],
#         password=configDict['database']['password'],
#         host=configDict['database']['server'],          
#         port=configDict['database']['port'],      
#         database=configDict['database']['database'],
#         query={"TrustServerCertificate": "YES", "driver": "ODBC Driver 18 for SQL Server"}   #Currently not been able to pass driver value from yaml as well as having TrustServerCertificate
#     )

# mssql_engine = create_engine(engine_url, echo=False, fast_executemany=True)


# data_source = context.data_sources.add_sql("daft db", mssql_engine)


data_source = context.data_sources.add_postgres(
    "postgres db", connection_string=connection_string
)
data_asset = data_source.add_table_asset(name="taxi data", table_name="nyc_taxi_data")

batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
batch = batch_definition.get_batch()


