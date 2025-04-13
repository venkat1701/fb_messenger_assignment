"""
Models for interacting with Cassandra tables.
"""
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    """

    @staticmethod
    async def create_message(sender_id: int, receiver_id: int, content: str) -> Dict[str, Any]:
        """
        Create a new message between users.

        Args:
            sender_id: ID of the sending user
            receiver_id: ID of the receiving user
            content: Message content

        Returns:
            Dictionary with message data
        """
        # Generate IDs and timestamps
        message_id = uuid.uuid4()
        created_at = datetime.utcnow()

        # Sort user IDs to create a consistent conversation ID
        user_ids = sorted([sender_id, receiver_id])
        conversation_id = uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{user_ids[0]}-{user_ids[1]}"
        )

        # Insert message into messages_by_conversation
        query = """
            INSERT INTO messages_by_conversation (
                conversation_id, 
                created_at, 
                message_id, 
                sender_id, 
                receiver_id, 
                content
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (conversation_id, created_at, message_id, sender_id, receiver_id, content)
        await cassandra_client.execute(query, params)

        # Update conversation for sender
        update_conversation_query = """
            INSERT INTO conversations_by_user (
                user_id, 
                conversation_id, 
                last_message_at, 
                other_user_id, 
                last_message_preview
            ) VALUES (%s, %s, %s, %s, %s)
        """
        # For sender
        sender_params = (sender_id, conversation_id, created_at, receiver_id, content[:50])
        await cassandra_client.execute(update_conversation_query, sender_params)

        # For receiver
        receiver_params = (receiver_id, conversation_id, created_at, sender_id, content[:50])
        await cassandra_client.execute(update_conversation_query, receiver_params)

        return {
            "message_id": message_id,
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "created_at": created_at
        }

    @staticmethod
    async def get_conversation_messages(
        conversation_id: int,
        page: int = 1,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get messages for a conversation with pagination.

        Args:
            conversation_id: ID of the conversation
            page: Page number, starting from 1
            limit: Number of messages per page

        Returns:
            List of message dictionaries
        """
        # Calculate offset
        offset = (page - 1) * limit

        query = """
            SELECT 
                message_id, 
                conversation_id, 
                sender_id, 
                receiver_id, 
                content, 
                created_at 
            FROM messages_by_conversation 
            WHERE conversation_id = %s 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        params = (conversation_id, limit)

        result = await cassandra_client.execute(query, params)
        return result

    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: int,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get messages before a timestamp with pagination.

        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number, starting from 1
            limit: Number of messages per page

        Returns:
            List of message dictionaries
        """
        query = """
            SELECT 
                message_id, 
                conversation_id, 
                sender_id, 
                receiver_id, 
                content, 
                created_at 
            FROM messages_by_conversation 
            WHERE conversation_id = %s 
            AND created_at < %s 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        params = (conversation_id, before_timestamp, limit)

        result = await cassandra_client.execute(query, params)
        return result


class ConversationModel:
    """
    Conversation model for interacting with conversations-related tables.
    """

    @staticmethod
    async def get_user_conversations(
        user_id: int,
        page: int = 1,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get conversations for a user with pagination.

        Args:
            user_id: ID of the user
            page: Page number, starting from 1
            limit: Number of conversations per page

        Returns:
            List of conversation dictionaries
        """
        query = """
            SELECT 
                conversation_id, 
                user_id,
                other_user_id,
                last_message_at, 
                last_message_preview 
            FROM conversations_by_user 
            WHERE user_id = %s 
            ORDER BY last_message_at DESC 
            LIMIT %s
        """
        params = (user_id, limit)

        result = await cassandra_client.execute(query, params)
        return result

    @staticmethod
    async def get_conversation(conversation_id: int) -> Dict[str, Any]:
        """
        Get a conversation by ID.

        Args:
            conversation_id: ID of the conversation

        Returns:
            Conversation data dictionary
        """
        # Get most recent message to extract user IDs
        query = """
            SELECT 
                conversation_id, 
                sender_id, 
                receiver_id, 
                content, 
                created_at 
            FROM messages_by_conversation 
            WHERE conversation_id = %s 
            LIMIT 1
        """
        params = (conversation_id,)

        result = await cassandra_client.execute(query, params)
        if not result:
            return None

        message = result[0]
        return {
            "id": conversation_id,
            "user1_id": message["sender_id"],
            "user2_id": message["receiver_id"],
            "last_message_at": message["created_at"],
            "last_message_content": message["content"]
        }