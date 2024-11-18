from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def consume_reddit_messages():
    consumer = KafkaConsumer(
        'reddit_raw',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='reddit-test-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    logger.info("Starting to consume Reddit messages...")
    
    try:
        for message in consumer:
            comment = message.value
            logger.info(f"""
            New comment received:
            Subreddit: r/{comment['subreddit']}
            Author: u/{comment['author']}
            Content: {comment['body'][:100]}...
            """)
            
    except KeyboardInterrupt:
        logger.info("Stopping the consumer (Ctrl+C pressed)")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_reddit_messages() 