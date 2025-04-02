import great_expectations as gx

import pandas as pd

print(gx.__version__)


# df = pd.read_csv(
#     "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
# )


df = pd.read_csv("yellow_tripdata_sample_2019-01.csv")


context = gx.get_context()

data_source = context.data_sources.add_pandas("pandas")
data_asset = data_source.add_dataframe_asset(name="pd dataframe asset")

batch_definition = data_asset.add_batch_definition_whole_dataframe("batch definition")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

expectation = gx.expectations.ExpectColumnValuesToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)

validation_result = batch.validate(expectation)

print(validation_result)