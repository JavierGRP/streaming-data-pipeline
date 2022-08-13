from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def writeToMongo(df, batchId):
        df.write \
        .format("mongo") \
        .option('uri', 'mongodb://127.0.0.1')\
        .option('database', 'spotifydb') \
        .option('collection', 'spotifycoll') \
        .mode("append") \
        .save()
        pass

kafka_topic_name = "spotifyTopic"
kafka_bootstrap_servers = "localhost:9092"

spark = SparkSession \
        .builder \
        .appName("Spotify streaming") \
        .master("local[*]") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/spotifydb.spotifycoll") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/spotifydb.spotifycoll") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

streamingDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .load()

stringDF = streamingDF.selectExpr("CAST(value AS STRING)")

schema = "name STRING, album STRING, artist STRING, danceability FLOAT"

csvDF = stringDF.select(from_csv(col("value"), schema).alias("data"))

schemaDF = csvDF.select("data.*")

finalDF = schemaDF.withColumn("partyHard?", when((schemaDF.danceability > 0.8), lit("ABSOLUTELY YESS!!!")) \
                                                .when((schemaDF.danceability >= 0.6) & \
                                                (schemaDF.danceability < 0.8), lit("Yes"))
                                                .when((schemaDF.danceability >= 0.4) & \
                                                (schemaDF.danceability < 0.6), lit("Maybe?")) \
                                                .when((schemaDF.danceability >= 0.3) & \
                                                (schemaDF.danceability < 0.5), lit("No"))
                                                .otherwise(lit("ABSOLUTELY NO!!!")))

query = finalDF \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .format("console") \
        .start()

# Send each row of the streaming DataFrame to writeToMongo function as RDD 
queryToMongo = finalDF.writeStream.foreachBatch(writeToMongo).start()
queryToMongo.awaitTermination()

query.awaitTermination()	
