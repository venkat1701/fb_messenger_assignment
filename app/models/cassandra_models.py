"""
Sample models for interacting with Cassandra tables.
Students should implement these models based on their database schema design.
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve messages
    - How to handle pagination of results
    - How to filter messages by timestamp
    """
    
    # TODO: Implement the following methods
    
    @staticmethod
    async def create_message(*args, **kwargs):
        """
        Create a new message.
        
        Students should decide what parameters are needed based on their schema design.
        """
        # This is a stub - students will implement the actual logic
        conversation_id = kwargs.get("conversation_id")
        sender_id = kwargs.get("sender_id")
        recipient_id = kwargs.get("recipient_id")
        context = kwargs.get("context")

        message_id = uuid.uuid4()
        timestamp = datetime.utcnow()

        message_in_conversation_query = """
            INSERT INTO messages_by_conversation (conversation_id, message_timestamp, message_id, sender_id, recipient_id, context)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        conversation_in_user_query = """
            INSERT INTO conversations_by_user(user_id, conversation_id, last_message_timestamp, participant_id, last_message_preview)
            VALUES (%s, %s, %s, %s, %s)
        """

        async with cassandra_client() as session:
            await session.execute(message_in_conversation_query, (conversation_id, timestamp, message_id, sender_id, recipient_id, context))
            await session.execute(conversation_in_user_query, (sender_id, conversation_id, timestamp, recipient_id, context))
            await session.execute(conversation_in_user_query, (recipient_id, conversation_id, timestamp, sender_id, context))

        return {
            "id": str(message_id),
            "sender_id": str(sender_id),
            "recipient_id": str(recipient_id),
            "conversation_id": str(conversation_id),
            "timestamp": timestamp,
            "context": context,
            "message_id": message_id,
        }

    @staticmethod
    async def get_conversation_messages(*args, **kwargs):
        """
        Get messages for a conversation with pagination.
        
        Students should decide what parameters are needed and how to implement pagination.
        """
        # This is a stub - students will implement the actual logic

        conversation_id = kwargs.get("conversation_id")
        limit = kwargs.get("limit", 20)

        async with cassandra_client() as session:
            result = await session.execute("""
                SELECT * FROM messages_by_conversation
                WHERE conversation_id = %s
                LIMIT %s
            """, (conversation_id, limit))
            return result.current_rows

    @staticmethod
    async def get_messages_before_timestamp(*args, **kwargs):
        """
        Get messages before a timestamp with pagination.
        
        Students should decide how to implement filtering by timestamp with pagination.
        """
        # This is a stub - students will implement the actual logic
        conversation_id = kwargs.get("conversation_id")
        before_timestamp = kwargs.get("before_timestamp")
        limit = kwargs.get("limit", 20)

        query = """
                   SELECT * FROM messages_by_conversation 
                   WHERE conversation_id = %s AND message_timestamp < %s 
                   LIMIT %s
               """

        async with cassandra_client() as session:
            result = await session.execute(query, (conversation_id, before_timestamp, limit))
            return result.current_rows


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    Students will implement this as part of the assignment.
    
    They should consider:
    - How to efficiently store and retrieve conversations for a user
    - How to handle pagination of results
    - How to optimize for the most recent conversations
    """
    
    # TODO: Implement the following methods
    
    @staticmethod
    async def get_user_conversations(*args, **kwargs):
        """
        Get conversations for a user with pagination.
        
        Students should decide what parameters are needed and how to implement pagination.
        """
        # This is a stub - students will implement the actual logic
        user_id = kwargs.get("user_id")
        limit = kwargs.get("limit", 20)
        before_timestamp = kwargs.get("before_timestamp")

        if before_timestamp:
            query = """
                SELECT * FROM conversations_by_user 
                WHERE user_id = %s AND last_message_timestamp < %s 
                LIMIT %s
            """
            params = (user_id, before_timestamp, limit)
        else:
            query = """
                SELECT * FROM conversations_by_user 
                WHERE user_id = %s 
                LIMIT %s
            """
            params = (user_id, limit)

        async with cassandra_client() as session:
            result = await session.execute(query, params)
            return result.current_rows

    @staticmethod
    async def get_conversation(*args, **kwargs):
        """
        Get a conversation by ID.
        
        Students should decide what parameters are needed and what data to return.
        """
        # This is a stub - students will implement the actual logic
        conversation_id = kwargs.get("conversation_id")
        limit = kwargs.get("limit", 20)

        query = """
           SELECT * FROM messages_by_conversation 
           WHERE conversation_id = %s 
           LIMIT %s
        """

        async with cassandra_client() as session:
            result = await session.execute(query, (conversation_id, limit))
            return result.current_rows

    @staticmethod
    async def create_or_get_conversation(*args, **kwargs):
        """
        Get an existing conversation between two users or create a new one.
        
        Students should decide how to handle this operation efficiently.
        """
        # This is a stub - students will implement the actual logic
        user1_id = kwargs.get("user1_id")
        user2_id = kwargs.get("user2_id")

        sorted_ids = sorted([str(user1_id), str(user2_id)])
        conversation_id = uuid.uuid5(uuid.NAMESPACE_DNS, f"{sorted_ids[0]}-{sorted_ids[1]}")

        check_query = """
                    SELECT conversation_id FROM messages_by_conversation 
                    WHERE conversation_id = %s LIMIT 1
                """

        async with cassandra_client() as session:
            result = await session.execute(check_query, (conversation_id,))
            rows = result.current_rows

        if rows:
            return {"new": False, "conversation_id": str(conversation_id)}
        else:
            return {"new": True, "conversation_id": str(conversation_id)}
