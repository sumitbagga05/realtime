## Below coins were used in this project.
##BTC,ETH,XRP,BCH,BSV,LTC,EOS,BNB,ADA,OKB,XLM,LINK,XMR,TRX,LEOTOKEN,HT,CRO,ETC,DASH,USDC,NEO,ATOM,IOT,XEM,FTXTOKEN,MKR,ONT,VET,BAT,PAX,DGB,MWC,BUSD,BTG,LSK,DCR,HBAR,ALGO,QTUM,ICX,THETA,BTM,RVN&interval=1d,7d,30d

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DecimalType, LongType
from pyspark.conf import SparkConf

####################################################################################################################
#Start the Spark Session
####################################################################################################################
spark = SparkSession.builder\
        .master(master="spark://node1.us-central1-a.c.second-casing-276006.internal:7077") \
        .config("spark.cassandra.connection.host", "10.128.0.4") \
        .config("spark.eventLog.enabled", "false") \
        .config("spark.driver.memory", "3g") \
        .config("spark.executor.memory", "3g") \
        .config("spark.jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.11-2.4.4.jar,/opt/spark/jars/kafka-clients-2.1.1.jar,/opt/spark/jars/spark-cassandra-connector_2.11-2.4.3.jar,/opt/spark/jars/spark-streaming_2.11-2.4.4.jar,/opt/spark/jars/jsr166e-1.1.0.jar") \
		.config("spark.executor.extraClassPath","/opt/spark/jars/spark-sql-kafka-0-10_2.11-2.4.4.jar,/opt/spark/jars/kafka-clients-2.1.1.jar,/opt/spark/jars/spark-cassandra-connector_2.11-2.4.3.jar,/opt/spark/jars/spark-streaming_2.11-2.4.4.jar,/opt/spark/jars/jsr166e-1.1.0.jar") \
        .config("spark.executor.extraLibrary","/opt/spark/jars/spark-sql-kafka-0-10_2.11-2.4.4.jar,/opt/spark/jars/kafka-clients-2.1.1.jar,/opt/spark/jars/spark-cassandra-connector_2.11-2.4.3.jar,/opt/spark/jars/spark-streaming_2.11-2.4.4.jar,/opt/spark/jars/jsr166e-1.1.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/spark-sql-kafka-0-10_2.11-2.4.4.jar,/opt/spark/jars/kafka-clients-2.1.1.jar,/opt/spark/jars/spark-cassandra-connector_2.11-2.4.3.jar,/opt/spark/jars/spark-streaming_2.11-2.4.4.jar,/opt/spark/jars/jsr166e-1.1.0.jar") \
        .appName(name="Spark Structured Streaming") \
        .getOrCreate()

###################################################################################################################
## To Create Schema.
####################################################################################################################
crypto_schema = StructType([
                        StructField('1d', StructType([
                                StructField('market_cap_change', StringType(), True),
                                StructField('market_cap_change_pct', StringType(), True),
                                StructField('price_change', StringType(), True),
                                StructField('price_change_pct', StringType(), True),
                                StructField('volume', StringType(), True),
                                StructField('volume_change', StringType(), True),
                                StructField('volume_change_pct',StringType(), True)
                                ])),
                        StructField('30d', StructType([
                                StructField('market_cap_change', StringType(), True),
                                StructField('market_cap_change_pct', StringType(), True),
                                StructField('price_change', StringType(), True),
                                StructField('price_change_pct', StringType(), True),
                                StructField('volume', StringType(), True),
                                StructField('volume_change', StringType(), True),
                                StructField('volume_change_pct',StringType(), True)
                                ])),
                        StructField('7d', StructType([
                                StructField('market_cap_change', StringType(), True),
                                StructField('market_cap_change_pct', StringType(), True),
                                StructField('price_change', StringType(), True),
                                StructField('price_change_pct', StringType(), True),
                                StructField('volume', StringType(), True),
                                StructField('volume_change', StringType(), True),
                                StructField('volume_change_pct',StringType(), True)
                                ])),
                        StructField('circulating_supply', StringType(), True),
                        StructField('currency', StringType(), True),
                        StructField('high', StringType(), True),
                        StructField('high_timestamp', StringType(), True),
                        StructField('id', StringType(), True),
                        StructField('logo_url', StringType(), True),
                        StructField('market_cap', StringType(), True),
                        StructField('max_supply', StringType(), True),
                        StructField('name', StringType(), True),
                        StructField('price', StringType(), True),
                        StructField('price_date', StringType(), True),
                        StructField('price_timestamp', StringType(), True),
                        StructField('rank', StringType(), True),
                        StructField('symbol', StringType(), True)])

#################################################################################################################
## To read the data in realtime from Kafka Topic
####################################################################################################################
df = spark.readStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092") \
                        .option("subscribe", "crypto-topic") \
                        .load() \
                        .select(from_json(col("value").cast("string"), crypto_schema).alias("crypto"))
df.printSchema()
##################################################################################################################
## Renamed the 1d,7d,30d and selected required Columns
## Transformation Area                                                                                                                                                  ##################################################################################################################
df2 = df.select("crypto.*").filter("rank is not NULL")
columns_df = df2.select("rank","name","market_cap","price","high","circulating_supply","max_supply","price_timestamp" \
                        ,col("1d.price_change").alias("price_change_1d") \
                        ,col("1d.volume").alias("volume_1d") \
                        ,col("1d.volume_change").alias("volume_change_1d") \
                        ,col("1d.market_cap_change").alias("market_cap_change_1d") \
                        ,col("7d.price_change").alias("price_change_7d") \
                        ,col("7d.volume").alias("volume_7d") \
                        ,col("7d.volume_change").alias("volume_change_7d") \
                        ,col("7d.market_cap_change").alias("market_cap_change_7d") \
                        ,col("30d.price_change").alias("price_change_30d") \
                        ,col("30d.volume").alias("volume_30d") \
                        ,col("30d.volume_change").alias("volume_change_30d") \
                        ,col("30d.market_cap_change").alias("market_cap_change_30d"))

####################################################################################################################
# To view realtime on console                                                                                   ####
####################################################################################################################

columns_df.writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("append") \
        .option("truncate", "true")\
        .format("console") \
        .start()

####################################################################################################################
## User Defined Function for moving the data into cassandra
####################################################################################################################

def writeToCassandra(writeDF, epochId):
    writeDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="cryptousd", keyspace="cryptoks") \
        .mode('append') \
        .save()

## Taking Data as batch to realtime
df3 = columns_df.writeStream \
                .trigger(processingTime="5 seconds") \
                .outputMode("update") \
                .foreachBatch(writeToCassandra) \
                .start()
df3.awaitTermination()
