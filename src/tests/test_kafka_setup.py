from kafka import KafkaProducer, KafkaConsumer
import json
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_TOPIC = 'reddit_raw'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

def test_kafka_connection():
    """Test basic Kafka producer and consumer functionality"""
    
    # First set up the consumer (before producing the message)
    logger.info("Setting up Kafka Consumer...")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='test-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )
    
    # Test message
    test_message = {
        'message': 'Test message',
        'timestamp': datetime.now().isoformat()
    }
    
    # Test Producer
    logger.info("Testing Kafka Producer...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Send test message
        future = producer.send(KAFKA_TOPIC, test_message)
        result = future.get(timeout=60)
        producer.flush()
        logger.info(f"✅ Producer test passed - Message sent: {test_message}")
        
    except Exception as e:
        logger.error(f"❌ Producer test failed: {str(e)}")
        return False
    finally:
        producer.close()
    
    # Test Consumer
    logger.info("Testing Kafka Consumer...")
    try:
        logger.info("Waiting for message (10 seconds timeout)...")
        message_received = False
        
        start_time = time.time()
        while time.time() - start_time < 10:  # 10 seconds timeout
            records = consumer.poll(timeout_ms=1000)
            if records:
                for topic_partition, messages in records.items():
                    for message in messages:
                        logger.info(f"✅ Message received: {message.value}")
                        message_received = True
                        break
            if message_received:
                break
            time.sleep(0.1)
            
        if not message_received:
            logger.error("❌ No message received within timeout period")
            return False
            
    except Exception as e:
        logger.error(f"❌ Consumer test failed: {str(e)}")
        return False
    finally:
        consumer.close()
    
    return True

if __name__ == "__main__":
    logger.info("Starting Kafka connection test...")
    
    success = test_kafka_connection()
    
    if success:
        logger.info("🎉 All Kafka tests passed successfully!")
    else:
        logger.error("❌ Kafka tests failed")