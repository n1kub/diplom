import findspark
# findspark.init()
findspark.add_packages("org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
import json
import pyspark.serializers as sz
from pyspark.sql.types import *
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *


def cons(topic: str):
    app = "Myapp"
    master = "local"

    spark = SparkSession.builder \
        .master(master) \
        .appName(app) \
        .getOrCreate()

    kafka_servers = "localhost:9092"

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .load()



    # .option("startingOffsets", "earliest") \
    schema = StructType([
        StructField('Request', StructType([
            StructField('directiveRegistry', StructType([
                StructField('directiveRegistryRecord', StructType([
                    StructField('directiveRecordId', StringType(), True),
                    StructField('directiveRecord', StructType([
                        StructField('Id', StringType(), True),
                        StructField('directiveRecordContent', StructType([
                            StructField('Id', StringType(), True),
                            StructField('BRAKZZPRequest', StructType([
                                StructField('ДатаСвед', StringType(), True),
                                StructField('ИдСвед', StringType(), True),
                                StructField('СведРегБрак', StructType([
                                    StructField('ДатаВерс', StringType(), True),
                                    StructField('ДатаЗапис', StringType(), True),
                                    StructField('НомерВерс', StringType(), True),
                                    StructField('НомерЗапис', StringType(), True),
                                    StructField('ОрганЗАГС', StructType([
                                        StructField('КодЗАГС', StringType(), True),
                                        StructField('НаимЗАГС', StringType(), True)
                                    ])),
                                    StructField('СтатусЗаписи', StructType([
                                        StructField('ДатаНачСтатус', StringType(), True),
                                        StructField('КодСтатус', StringType(), True),
                                        StructField('НаимСтатус', StringType(), True)
                                    ])),
                                    StructField('ПрдСведРег', StructType([
                                        StructField('ДатаЗклБрак', StringType(), True),
                                        StructField('СведЛицБрак', StructType([
                                            StructField('Супруг', StructType([
                                                StructField('ФамилияДо', StringType(), True),
                                                StructField('ФамилияПосле', StringType(), True),
                                                StructField('Имя', StringType(), True),
                                                StructField('Отчество', StringType(), True),
                                                StructField('Гражданство', StructType([
                                                    StructField('НаимСтраны', StringType(), True),
                                                    StructField('ОКСМ', StringType(), True)
                                                ])),
                                                StructField('ДатаРождКаленд', StringType(), True),
                                                StructField('МестоРожден', StructType([
                                                    StructField('Город', StringType(), True),
                                                    StructField('КодСтраны', StringType(), True),
                                                    StructField('МестоТекст', StringType(), True),
                                                    StructField('НаимСтраны', StringType(), True),
                                                    StructField('НаимСубъект', StringType(), True),
                                                    StructField('ПризнМесто', StringType(), True),
                                                    StructField('Регион', StringType(), True)
                                                ])),
                                                StructField('АдрМЖ', StructType([
                                                    StructField('АдрМЖРФ', StructType([
                                                        StructField('АдрРФТекст', StringType(), True),
                                                        StructField('ПрТипАдрРФ', StringType(), True),
                                                        StructField('АдрКЛАДР', StructType([
                                                            StructField('Город', StringType(), True),
                                                            StructField('Дом', StringType(), True),
                                                            StructField('Индекс', StringType(), True),
                                                            StructField('Кварт', StringType(), True),
                                                            StructField('КодРегион', StringType(), True),
                                                            StructField('Корпус', StringType(), True),
                                                            StructField('НаимРегион', StringType(), True),
                                                            StructField('Улица', StringType(), True)
                                                        ]))
                                                    ]))
                                                ])),
                                                StructField('УдЛичнФЛ', StructType([
                                                    StructField('ВыдДок', StringType(), True),
                                                    StructField('ДатаДок', StringType(), True),
                                                    StructField('КодВидДок', StringType(), True),
                                                    StructField('НаимДок', StringType(), True),
                                                    StructField('СерНомДок', StringType(), True)
                                                ]))
                                            ])),
                                            StructField('Супруга', StructType([
                                                StructField('ФамилияДо', StringType(), True),
                                                StructField('ФамилияПосле', StringType(), True),
                                                StructField('Имя', StringType(), True),
                                                StructField('Отчество', StringType(), True),
                                                StructField('Гражданство', StructType([
                                                    StructField('НаимСтраны', StringType(), True),
                                                    StructField('ОКСМ', StringType(), True)
                                                ])),
                                                StructField('ДатаРождКаленд', StringType(), True),
                                                StructField('МестоРожден', StructType([
                                                    StructField('Город', StringType(), True),
                                                    StructField('КодСтраны', StringType(), True),
                                                    StructField('МестоТекст', StringType(), True),
                                                    StructField('НаимСтраны', StringType(), True),
                                                    StructField('НаимСубъект', StringType(), True),
                                                    StructField('ПризнМесто', StringType(), True),
                                                    StructField('Регион', StringType(), True)
                                                ])),
                                                StructField('АдрМЖ', StructType([
                                                    StructField('АдрМЖРФ', StructType([
                                                        StructField('АдрРФТекст', StringType(), True),
                                                        StructField('ПрТипАдрРФ', StringType(), True),
                                                        StructField('АдрКЛАДР', StructType([
                                                            StructField('Город', StringType(), True),
                                                            StructField('Дом', StringType(), True),
                                                            StructField('Индекс', StringType(), True),
                                                            StructField('Кварт', StringType(), True),
                                                            StructField('КодРегион', StringType(), True),
                                                            StructField('Корпус', StringType(), True),
                                                            StructField('НаимРегион', StringType(), True),
                                                            StructField('Улица', StringType(), True)
                                                        ]))
                                                    ]))
                                                ])),
                                                StructField('УдЛичнФЛ', StructType([
                                                    StructField('ВыдДок', StringType(), True),
                                                    StructField('ДатаДок', StringType(), True),
                                                    StructField('КодВидДок', StringType(), True),
                                                    StructField('НаимДок', StringType(), True),
                                                    StructField('СерНомДок', StringType(), True)
                                                ]))
                                            ]))
                                        ]))
                                    ]))
                                ]))
                            ]))
                        ]))
                    ]))
                ]))
            ]))
        ]))
    ])

    # d = json.loads()
    # d = df.values()
    # df.select(functions.from_json(df.value))
    # df.select(functions.from_json(functions.col("value").cast("string"), schema).alias("value")).collect()
    df = df.withColumn("value", decode(df.value, "UTF-8"))
    # schema = StructType([
    #     StructField("data", StringType(), True),
    # ])
    # print(df.collect()[4][1])
    # df.printSchema()
    # df.show(100, truncate=False)
    # dfr = df.select("value")
    # df.select(from_json(col("value").cast("string").alias("json"), schema)).collect()
    dfData = df.withColumn("jsonData", from_json(df.value, schema, {"mode" : "FAILFAST"})).select("jsonData.*")
    # dfData = spark.read.json(dfr.rdd.map(lambda j: j.value), multiLine=True)
    # dfData = df.select(from_json(df.value.cast("string"), schema_of_json(lit(col("value")))).alias("data1"))
    # dfData = df.withColumn("value", from_json(df.value, schema)).select("value.*")
    # dfData = df.select(col("value"), json_tuple(col("value"), "data","additional_data",)).toDF("value","data","additional_data")
    # dfData.printSchema()
    # dfData.show(100, truncate=False)
    dfData.writeStream.outputMode("append").format("console").start().awaitTermination()
    husbandBefore = dfData.collect()[0][0][0][0][1][1][1][2][6][1][0][0]
    husbandAfter = dfData.collect()[0][0][0][0][1][1][1][2][6][1][0][1]
    wifeBefore = dfData.collect()[0][0][0][0][1][1][1][2][6][1][1][0]
    wifendAfter = dfData.collect()[0][0][0][0][1][1][1][2][6][1][1][1]
    print(husbandBefore,husbandAfter,wifeBefore,wifendAfter, sep='\n')

    # df.show(truncate=False)
    # data = df.select('value')
    # d = spark.read.json(df.rdd.map(lambda x: x[0]), multiLine=True)
    # data.show(truncate=False)
    # data.columns[0].
    # d.show(truncate=False)

