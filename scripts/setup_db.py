"""
Script to initialize Cassandra keyspace and tables for the Messenger application.
"""
import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

def wait_for_cassandra():
    """Wait for Cassandra to be ready before proceeding."""
    logger.info("Waiting for Cassandra to be ready...")
    cluster = None
    
    for _ in range(10):  # Try 10 times
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            logger.info("Cassandra is ready!")
            return cluster
        except Exception as e:
            logger.warning(f"Cassandra not ready yet: {str(e)}")
            time.sleep(5)  # Wait 5 seconds before trying again
    
    logger.error("Failed to connect to Cassandra after multiple attempts.")
    raise Exception("Could not connect to Cassandra")

def create_keyspace(session):
    """Create the keyspace if it doesn't exist."""
    logger.info(f"Creating keyspace {CASSANDRA_KEYSPACE} if it doesn't exist...")

    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': '1'
        }}
    """)
    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} is ready.")

def create_tables(session):
    """Create the tables for the application."""
    logger.info("Creating tables...")

    # Table for storing messages by conversation (for fetching message history)
    session.execute("""
        CREATE TABLE IF NOT EXISTS messages_by_conversation (
            conversation_id uuid,
            created_at timestamp,
            message_id uuid,
            sender_id int,
            receiver_id int,
            content text,
            PRIMARY KEY ((conversation_id), created_at, message_id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, message_id ASC)
    """)

    # Table for storing user's conversations (for fetching conversation list)
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations_by_user (
            user_id int,
            conversation_id uuid,
            last_message_at timestamp,
            other_user_id int,
            last_message_preview text,
            PRIMARY KEY ((user_id), last_message_at, conversation_id)
        ) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC)
    """)

    # Table for storing user data
    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id int PRIMARY KEY,
            username text,
            full_name text,
            profile_pic_url text
        )
    """)

    logger.info("Tables created successfully.")

def main():
    """Initialize the database."""
    logger.info("Starting Cassandra initialization...")

    # Wait for Cassandra to be ready
    cluster = wait_for_cassandra()

    try:
        # Connect to the server
        session = cluster.connect()

        # Create keyspace and tables
        create_keyspace(session)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        create_tables(session)

        logger.info("Cassandra initialization completed successfully.")
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        raise
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()