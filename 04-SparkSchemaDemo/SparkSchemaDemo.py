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

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", "true") \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.csv")

    # flightTimeCsvDF.printSchema()

    flightTimeCsvDF.show(5)
    logger.info("CSV Schema:" + flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.json")

    flightTimeJsonDF.show(5)
    logger.info("JSON Schema:" + flightTimeJsonDF.schema.simpleString())

    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    # flightTimeParquetDF.printSchema()

    flightTimeParquetDF.show(5)
    logger.info("Parquet Schema:" + flightTimeParquetDF.schema.simpleString())

    cms1JsonSchemaStruct = StructType([
        StructField("messageid", StringType()),
        StructField("simulationid", StringType()),
        StructField("creationtimestamp", DateType()),
        StructField("sendtimestamp", DateType()),
        StructField("vin", StringType()),
        StructField("tripid", StringType()),
        StructField("driverid", StringType()),
        StructField("geolocation", StructType([
            StructField("latitude", DecimalType()),
            StructField("longitude", DecimalType()),
            StructField("altitude", IntegerType()),
            StructField("heading", DecimalType()),
            StructField("speed", IntegerType()),
            StructField("location", ArrayType(DecimalType())),
        ])),
    ])

    cms1JsonDF = spark.read \
        .format("json") \
        .schema(cms1JsonSchemaStruct) \
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z'") \
        .load("data/cms1.json")

    # cms1JsonDF.printSchema()

    cms1JsonDF.show()

    cms2JsonSchemaStruct = StructType([
        StructField("objectid", StringType()),
        StructField("type", StringType()),
        StructField("event", StringType()),
        StructField("time", DateType())
    ])

    cms2JsonDF = spark.read \
        .format("json") \
        .schema(cms2JsonSchemaStruct) \
        .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z'") \
        .load("data/cms2.json")

    # cms2JsonDF.printSchema()

    cms2JsonDF.show()

    schemaJson = cms1JsonDF.schema.json()

    with open('data/cms1Schema.json', 'w', encoding='utf-8') as f:
        f.write(str(schemaJson))
        # alternatives:
        # json.dump(json.loads(schemaJson), f, ensure_ascii=False, indent=4)
