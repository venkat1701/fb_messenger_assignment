from fastapi import HTTPException, status

from app.models.cassandra_models import MessageModel
from app.schemas.message import (
    MessageCreate,
    MessageResponse,
    PaginatedMessageResponse,
)


class MessageController:
    """
    Controller for handling message-related operations
    """

    async def create_message(self, message: MessageCreate) -> MessageResponse:
        """
        Create a new message in a conversation.

        Args:
            message: MessageCreate object

        Returns:
            MessageResponse
        """
        try:
            created = await MessageModel.create_message(
                sender_id=message.sender_id,
                receiver_id=message.receiver_id,
                content=message.content,
            )

            return MessageResponse(
                id=created["message_id"],
                sender_id=created["sender_id"],
                receiver_id=created["receiver_id"],
                content=created["content"],
                created_at=created["created_at"],
                conversation_id=created["conversation_id"],
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create message: {str(e)}"
            )

    async def get_conversation_messages(
        self,
        conversation_id: int,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get paginated messages from a conversation.

        Args:
            conversation_id: ID of the conversation
            page: Page number
            limit: Page size

        Returns:
            PaginatedMessageResponse
        """
        try:
            messages = await MessageModel.get_conversation_messages(
                conversation_id=conversation_id,
                page=page,
                limit=limit
            )

            response_data = [
                MessageResponse(
                    id=msg["message_id"],
                    sender_id=msg["sender_id"],
                    receiver_id=msg["receiver_id"],
                    content=msg["content"],
                    created_at=msg["created_at"],
                    conversation_id=msg["conversation_id"],
                )
                for msg in messages
            ]

            return PaginatedMessageResponse(
                total=len(response_data),
                page=page,
                limit=limit,
                data=response_data
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch messages: {str(e)}"
            )

    async def get_messages_before_timestamp(
        self,
        conversation_id: int,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages from a conversation before a timestamp (for infinite scroll).

        Args:
            conversation_id: ID of the conversation
            before_timestamp: Upper bound for message timestamps
            page: Page number
            limit: Page size

        Returns:
            PaginatedMessageResponse
        """
        try:
            messages = await MessageModel.get_messages_before_timestamp(
                conversation_id=conversation_id,
                before_timestamp=before_timestamp,
                page=page,
                limit=limit
            )

            response_data = [
                MessageResponse(
                    id=msg["message_id"],
                    sender_id=msg["sender_id"],
                    receiver_id=msg["receiver_id"],
                    content=msg["content"],
                    created_at=msg["created_at"],
                    conversation_id=msg["conversation_id"],
                )
                for msg in messages
            ]

            return PaginatedMessageResponse(
                total=len(response_data),
                page=page,
                limit=limit,
                data=response_data
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch messages: {str(e)}"
            )
