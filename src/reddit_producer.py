import praw
import json
from kafka import KafkaProducer
from datetime import datetime
import logging
from dotenv import load_dotenv
import os
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RedditProducer:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Initialize Reddit client
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        self.topic = 'reddit_raw'
        logger.info("Reddit Producer initialized successfully")

    def format_comment(self, comment):
        """Format comment data for Kafka"""
        return {
            'id': comment.id,
            'body': comment.body,
            'author': str(comment.author),
            'subreddit': str(comment.subreddit),
            'created_utc': comment.created_utc,
            'score': comment.score,
            'timestamp': datetime.utcnow().isoformat()
        }

    def stream_comments(self, subreddit_name='all'):
        """Stream comments from Reddit to Kafka"""
        try:
            subreddit = self.reddit.subreddit(subreddit_name)
            logger.info(f"Starting to stream comments from r/{subreddit_name}")
            
            # First, let's verify we can access the subreddit
            subreddit.id
            logger.info("Successfully connected to subreddit")
            
            # Get some initial comments to verify everything works
            logger.info("Fetching initial comments...")
            initial_comments = subreddit.comments(limit=5)
            for comment in initial_comments:
                comment_data = self.format_comment(comment)
                self.producer.send(self.topic, comment_data)
                logger.info(f"Sent initial comment from r/{comment_data['subreddit']}")
            
            logger.info("Starting continuous stream...")
            comment_count = 0
            start_time = time.time()
            
            # Main streaming loop
            for comment in subreddit.stream.comments(skip_existing=True):
                try:
                    comment_data = self.format_comment(comment)
                    self.producer.send(self.topic, comment_data)
                    comment_count += 1
                    
                    # Log progress every few comments
                    if comment_count % 5 == 0:
                        elapsed_time = time.time() - start_time
                        rate = comment_count / elapsed_time
                        logger.info(f"Processed {comment_count} comments ({rate:.2f} comments/second)")
                        
                    # Log individual comments
                    body_preview = comment_data['body'][:50] + '...'
                    logger.info(f"New comment from r/{comment_data['subreddit']}: {body_preview}")
                    
                except Exception as e:
                    logger.error(f"Error processing comment: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Stream error: {str(e)}")
            raise

    def close(self):
        """Clean up resources"""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed successfully")

def main():
    producer = None
    try:
        producer = RedditProducer()
        # Let's try a more active subreddit
        producer.stream_comments('AskReddit')  # AskReddit typically has more activity
        
    except KeyboardInterrupt:
        logger.info("Stopping the producer (Ctrl+C pressed)")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()