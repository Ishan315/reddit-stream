import subprocess
import time
import os
from kafka_config import TOPIC_CONFIGS

class KafkaManager:
    def __init__(self):
        self.kafka_path = os.path.expanduser('~/kafka/kafka_2.13-3.6.1')
        
    def start_kafka(self):
        try:
            # Start Zookeeper
            print("Starting Zookeeper...")
            zk_cmd = f"{self.kafka_path}/bin/zookeeper-server-start.sh"
            zk_config = f"{self.kafka_path}/config/zookeeper.properties"
            subprocess.Popen([zk_cmd, zk_config],
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            time.sleep(5)

            # Start Kafka
            print("Starting Kafka...")
            kafka_cmd = f"{self.kafka_path}/bin/kafka-server-start.sh"
            kafka_config = f"{self.kafka_path}/config/server.properties"
            subprocess.Popen([kafka_cmd, kafka_config],
                           stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            time.sleep(5)

            print("Kafka services started successfully!")
            return True

        except Exception as e:
            print(f"Error starting Kafka services: {str(e)}")
            return False

    def setup_topics(self):
        for topic_name, config in TOPIC_CONFIGS.items():
            self.create_topic(topic_name, config)

    def create_topic(self, topic_name, config):
        try:
            topic_cmd = f"{self.kafka_path}/bin/kafka-topics.sh"
            subprocess.run([
                topic_cmd,
                '--create',
                '--topic', topic_name,
                '--bootstrap-server', 'localhost:9092',
                '--partitions', str(config['partitions']),
                '--replication-factor', str(config['replication_factor'])
            ], check=True)
            print(f"Topic '{topic_name}' created successfully!")
            return True
        except subprocess.CalledProcessError as e:
            if "already exists" in str(e):
                print(f"Topic '{topic_name}' already exists.")
                return True
            print(f"Error creating topic: {str(e)}")
            return False

if __name__ == "__main__":
    kafka_manager = KafkaManager()
    if kafka_manager.start_kafka():
        kafka_manager.setup_topics()