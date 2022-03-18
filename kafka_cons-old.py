import pyspark.sql as pss
import pyspark
from pyspark.streaming import StreamingContext
# from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from kafka import KafkaConsumer
import json
import time
import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = \
#     '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 kafka_cons.py'

def cons(topic: str):
    # consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
    #                          value_deserializer=lambda x: json.loads(x))
    #, auto_offset_reset='earliest')
    spark, ssc = startSpark()

    lines = ssc.socketTextStream("localhost", 9092)
    # print(lines)
    lines.pprint(10)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    # counts.pprint(10)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
    # print(1)
    # for msg in consumer:
    #     print(2)
    #     print(msg)
    #     sparkss(msg.value, spark)


def startSpark():
    # print(3)
    # spark = pss.SparkSession.builder.master("local[*]").appName("MyPSpark").getOrCreate()
    # print(4)
    spark = pyspark.SparkContext("local[*]", "MySpark")

    ssc = StreamingContext(spark, 1)
    return spark, ssc


def sparkss(datafile, spark):

    # df = spark.read.json(datafile, multiLine=True)
    df = spark.\
        readStream.\
        format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "HI") \
        .option("startingOffsets", "earliest") \
        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    # print(type(df))
    # print(type(ds))
    df.printSchema()
    df.show(truncate=False)
