from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType

# Spark session and context
conf = SparkConf().setAppName("app")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.master("local").getOrCreate()

# Schema to enforce
schema = StructType([
    StructField("a", IntegerType(), False),
    StructField("b", IntegerType(), False),
    ])

# read the file as an rdd
rdd  = sc.textFile("./sample.csv")

# enforce the schema
rdd_with_schema = spark.read.schema(schema).csv(rdd)

print (f"schema= {rdd_with_schema.schema}")

"""
output: 
schema= StructType(
    List(
        StructField(a,IntegerType,false),
        StructField(b,IntegerType,false))
    )
"""
