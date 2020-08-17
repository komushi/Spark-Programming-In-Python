from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, ArrayType, StructField, DateType, StringType, IntegerType, DecimalType
import json

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    with open("data/cms1Schema.json") as json_file:
        schemaJson = json.load(json_file)

    cms1SchemaFromJson = StructType.fromJson(schemaJson)
    cms1JsonDF = spark.read \
        .format("json") \
        .schema(cms1SchemaFromJson) \
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z'") \
        .load("data/cms1.json")

    cms1JsonDF.printSchema()

    cms1JsonDF.show()
