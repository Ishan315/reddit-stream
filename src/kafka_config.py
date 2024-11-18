import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Path Configuration
# KAFKA_HOME = '/Users/jui/Desktop/Kafka/kafka_2.13-3.9.0'

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_RAW = 'reddit_raw'
KAFKA_TOPIC_PROCESSED = 'reddit_processed'

# Kafka Consumer Group
KAFKA_CONSUMER_GROUP = 'reddit_processor_group'

# Kafka Topics Configuration
TOPIC_CONFIGS = {
    'reddit_raw': {
        'partitions': 1,
        'replication_factor': 1
    },
    'reddit_processed': {
        'partitions': 1,
        'replication_factor': 1
    }
}