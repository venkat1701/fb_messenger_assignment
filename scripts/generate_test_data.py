import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10
NUM_CONVERSATIONS = 15
MAX_MESSAGES_PER_CONVERSATION = 50

def connect_to_cassandra():
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        session.set_keyspace(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_users(session):
    logger.info("Creating test users...")
    for user_id in range(1, NUM_USERS + 1):
        name = f"User{user_id}"
        session.execute(
            "INSERT INTO users (user_id, name) VALUES (%s, %s)",
            (user_id, name)
        )

def generate_conversations(session):
    logger.info("Creating conversations and messages...")
    for _ in range(NUM_CONVERSATIONS):
        participants = random.sample(range(1, NUM_USERS + 1), 2)
        conversation_id = uuid.uuid4()

        session.execute(
            "INSERT INTO conversations (conversation_id, participant_ids) VALUES (%s, %s)",
            (conversation_id, set(participants))
        )

        num_messages = random.randint(10, MAX_MESSAGES_PER_CONVERSATION)
        base_time = datetime.utcnow()

        for i in range(num_messages):
            created_at = base_time - timedelta(minutes=i)
            sender_id = random.choice(participants)
            receiver_id = participants[0] if sender_id == participants[1] else participants[1]
            content = f"Message {i + 1} from User{sender_id} to User{receiver_id}"

            session.execute(
                """
                INSERT INTO messages_by_conversation (
                    conversation_id, created_at, message_id, sender_id, receiver_id, content
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (conversation_id, created_at, uuid.uuid4(), sender_id, receiver_id, content)
            )

def generate_test_data(session):
    logger.info("Generating test data...")
    generate_users(session)
    generate_conversations(session)
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    cluster = None
    try:
        cluster, session = connect_to_cassandra()
        generate_test_data(session)
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main()
