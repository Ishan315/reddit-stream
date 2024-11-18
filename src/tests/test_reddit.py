import os
from dotenv import load_dotenv
import praw

def test_reddit_connection():
    # Load environment variables
    load_dotenv()
    
    try:
        # Create Reddit instance
        reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        # Test the connection by getting 1 hot post from r/python
        print("Attempting to fetch a post from r/python...")
        for submission in reddit.subreddit('python').hot(limit=1):
            print("\nConnection Successful! Here's a sample post:")
            print(f"Title: {submission.title}")
            print(f"Author: u/{submission.author}")
            print(f"Score: {submission.score}")
            print(f"URL: {submission.url}")
            
        print("\nReddit API connection test passed!")
        return True
        
    except Exception as e:
        print(f"\nError connecting to Reddit API: {str(e)}")
        print("\nPlease check your credentials in the .env file.")
        return False

if __name__ == "__main__":
    test_reddit_connection()