from dotenv import load_dotenv
import os
import time
import json
import praw
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

load_dotenv()

# Environment variables
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = os.getenv('TOPIC_NAME')

def create_reddit_instance():
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )
    try:
        print(reddit.user.me())
    except Exception as e:
        print(f"Auth check failed (expected for read-only?): {e}")
    return reddit

def create_kafka_producer():
    retries = 5
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[os.getenv('KAFKA_BROKER')],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka broker not found. Retries left: {retries}")
            retries -= 1
            time.sleep(5)
    raise RuntimeError("Failed to connect to Kafka after retries")

def main():
    reddit = create_reddit_instance()
    try:
        producer = create_kafka_producer()
    except Exception as e:
        print(f"Exiting due to Kafka error: {e}")
        return

    subreddits = ["technology", "programming", "datascience", "MachineLearning"]

    try:
        while True:
            for subreddit in subreddits:
                try:
                    print(f"Fetching r/{subreddit}")
                    for submission in reddit.subreddit(subreddit).new(limit=10):
                        data = {
                            "id": submission.id,
                            "subreddit": subreddit,
                            "title": submission.title,
                            "selftext": submission.selftext,
                            "created_utc": submission.created_utc
                        }
                        print(f"Sending post: {submission.title[:50]}...")
                        future = producer.send(TOPIC_NAME, value=data)
                        future.add_callback(lambda rm: print(f"Sent to {rm.topic}"))
                        future.add_errback(lambda e: print(f"Error: {e}"))
                except Exception as e:
                    print(f"Error in r/{subreddit}: {e}")
                    continue
            print("Waiting...")
            time.sleep(30)
    except KeyboardInterrupt:
        print("Exiting...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()