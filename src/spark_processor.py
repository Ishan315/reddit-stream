from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, from_json, col, explode, array, 
    to_json, struct, window, count, desc, current_timestamp
)
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, MapType
from elasticsearch import Elasticsearch
import spacy
import logging
import time
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_entities(text):
    """Standalone entity extraction function"""
    try:
        if not text or not isinstance(text, str):
            return []
        nlp = spacy.load("en_core_web_sm")
        doc = nlp(text)
        entities = [{"text": ent.text, "label": ent.label_} for ent in doc.ents]
        return entities
    except Exception as e:
        logger.error(f"Error in entity extraction: {str(e)}")
        return []

class SparkStreamProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RedditStreamProcessor") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," + 
                   "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.0") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.es = self.connect_with_retry("http://elasticsearch:9200")
        
        self.comment_schema = StructType([
            StructField("body", StringType(), True),
            StructField("created_utc", StringType(), True),
            StructField("subreddit", StringType(), True)
        ])

    def connect_with_retry(self, host, max_retries=5, delay=5):
        for i in range(max_retries):
            try:
                es = Elasticsearch([host], verify_certs=False)
                if es.ping():
                    logger.info("Successfully connected to Elasticsearch")
                    return es
            except Exception as e:
                logger.error(f"Connection attempt {i+1} failed: {str(e)}")
                if i < max_retries - 1:
                    time.sleep(delay)
                    continue
                raise
        raise ConnectionError("Could not connect to Elasticsearch after multiple retries")

    def process_batch(self, df, epoch_id):
        try:
            rows = df.collect()
            batch_data = []
            for row in rows:
                doc = {
                    "entity": row.entity,
                    "entity_type": row.entity_type,
                    "count": row.count,
                    "window_start": str(row.window.start),
                    "window_end": str(row.window.end),
                    "timestamp": time.time()
                }
                batch_data.append(doc)
            
            if batch_data:
                for doc in batch_data:
                    self.es.index(
                        index="reddit_entities",
                        document=doc
                    )
                logger.info(f"Processed batch {epoch_id}: {len(batch_data)} records")
        except Exception as e:
            logger.error(f"Error processing batch {epoch_id}: {str(e)}")

    def process_stream(self):
        try:
            # Create broadcast variable for spaCy model
            extract_entities_udf = udf(extract_entities, ArrayType(MapType(StringType(), StringType())))
            
            input_df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9093") \
                .option("subscribe", "reddit_raw") \
                .option("startingOffsets", "latest") \
                .load()
            
            parsed_df = input_df \
                .selectExpr("CAST(value AS STRING) as json_data") \
                .select(from_json("json_data", self.comment_schema).alias("data")) \
                .select(
                    "data.body",
                    current_timestamp().alias("timestamp")
                )
            
            entities_df = parsed_df \
                .withColumn("entities", extract_entities_udf(col("body"))) \
                .select(
                    "timestamp",
                    explode("entities").alias("entity_struct")
                ) \
                .select(
                    "timestamp",
                    col("entity_struct.text").alias("entity"),
                    col("entity_struct.label").alias("entity_type")
                )
            
            windowed_counts = entities_df \
                .withWatermark("timestamp", "5 minutes") \
                .groupBy(
                    window("timestamp", "15 minutes", "5 minutes"),
                    "entity",
                    "entity_type"
                ) \
                .count() \
                .limit(10)
            
            kafka_output = windowed_counts \
                .select(to_json(struct("*")).alias("value"))
            
            kafka_query = kafka_output \
                .writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9093") \
                .option("topic", "reddit_processed") \
                .option("checkpointLocation", "/tmp/checkpoint/kafka") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            es_query = windowed_counts \
                .writeStream \
                .foreachBatch(self.process_batch) \
                .trigger(processingTime='20 seconds') \
                .option("checkpointLocation", "/tmp/checkpoint/es") \
                .start()
            
            logger.info("Stream processing started")
            return [kafka_query, es_query]
            
        except Exception as e:
            logger.error(f"Error in stream processing: {str(e)}")
            raise

def main():
    processor = None
    queries = []
    
    try:
        processor = SparkStreamProcessor()
        queries = processor.process_stream()
        
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        logger.info("Stopping the processor (Ctrl+C pressed)")
        for query in queries:
            query.stop()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
    finally:
        if processor and processor.spark:
            processor.spark.stop()

if __name__ == "__main__":
    main()