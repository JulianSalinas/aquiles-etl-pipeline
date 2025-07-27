import sys
sys.path.append("/Workspace/Repos/<user-name>/<repo-name>")

from pyspark.sql.functions import udf
from pyspark.sql.types import DecimalType, StringType, StructField, StructType
from common.transforms import *


# Define the schema for the returned STRUCT type
measure_unit_schema = StructType([
    StructField("Measure", StringType(), True),
    StructField("UnitOfMeasure", StringType(), True),
    StructField("PackageUnits", StringType(), True)
])

# Register the UDFs
infer_and_transform_date_udf = udf(infer_and_transform_date, StringType())
transform_price_udf = udf(transform_price, DecimalType())
remove_special_characters_udf = udf(remove_special_characters, StringType())
separate_camel_case_udf = udf(separate_camel_case, StringType())
transform_provider_name_udf = udf(transform_provider_name, StringType())
extract_measure_and_unit_udf = udf(extract_measure_and_unit, measure_unit_schema)