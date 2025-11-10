import pandas as pd
import time
import os
from dotenv import load_dotenv # to load environment variables from a .env file stored in same root folder
from typing import Dict, Any, Optional, Union, Tuple
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

# Load environment variables
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")

class GenieClient:
    def __init__(self, host: str, space_id: str, token: str):
        self.host = host
        self.space_id = space_id
        self.token = token
        
        # Configure SDK with retry settings and explicit PAT auth
        config = Config(
            host=f"https://{host}",
            token=token,
            auth_type="pat",  # Explicitly set authentication type to PAT
            retry_timeout_seconds=180,  # 3 minutes total retry timeout
            max_retries=2,              # Maximum number of retries
            retry_delay_seconds=10,      # Initial delay between retries
            retry_backoff_factor=2      # Exponential backoff factor
        )
        
        self.client = WorkspaceClient(config=config)
    
    def start_conversation(self, question: str) -> Dict[str, Any]:
        """Start a new conversation with the given question"""
        response = self.client.genie.start_conversation(
            space_id=self.space_id,
            content=question
        )
        return {
            "conversation_id": response.conversation_id,
            "message_id": response.message_id
        }
    
    def send_message(self, conversation_id: str, message: str) -> Dict[str, Any]:
        """Send a follow-up message to an existing conversation"""
        response = self.client.genie.create_message(
            space_id=self.space_id,
            conversation_id=conversation_id,
            content=message
        )
        return {
            "message_id": response.message_id
        }
    
    def upload_message_attachment(self, conversation_id: str, message_id: str, file: bytes, filename: str):
        """Upload an attachment to a specific Genie message"""
        return self.client.genie.upload_message_attachment(
            space_id=self.space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            file=file,
            name=filename
        )

    def get_message(self, conversation_id: str, message_id: str) -> Dict[str, Any]:
        """Get the details of a specific message"""
        response = self.client.genie.get_message(
            space_id=self.space_id,
            conversation_id=conversation_id,
            message_id=message_id
        )
        return response.as_dict()

    def get_query_result(self, conversation_id: str, message_id: str, attachment_id: str) -> Dict[str, Any]:
        """Get the query result using the attachment_id endpoint"""
        response = self.client.genie.get_message_attachment_query_result(
            space_id=self.space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            attachment_id=attachment_id
        )
        
        # Extract data_array from the correct nested location
        data_array = []
        if hasattr(response, 'statement_response') and response.statement_response is not None:
            if (hasattr(response.statement_response, 'result') and 
                response.statement_response.result is not None):
                data_array = response.statement_response.result.data_array or []
            else:
                raise ValueError("Query execution failed: No result data available. The query may have failed or returned no data.")
        else:
            raise ValueError("Query execution failed: No statement response available from the server.")
        
        # Extract schema safely
        schema = {}
        if (hasattr(response, 'statement_response') and response.statement_response is not None and
            hasattr(response.statement_response, 'manifest') and response.statement_response.manifest is not None and
            hasattr(response.statement_response.manifest, 'schema') and response.statement_response.manifest.schema is not None):
            schema = response.statement_response.manifest.schema.as_dict()
            
        return {
            'data_array': data_array,
            'schema': schema
        }

    def execute_query(self, conversation_id: str, message_id: str, attachment_id: str) -> Dict[str, Any]:
        """Execute a query using the attachment_id endpoint"""
        response = self.client.genie.execute_message_attachment_query(
            space_id=self.space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            attachment_id=attachment_id
        )
        return response.as_dict()

    def wait_for_message_completion(self, conversation_id: str, message_id: str, timeout: int = 300, poll_interval: int = 2) -> Dict[str, Any]:
        """
        Wait for a message to reach a terminal state (COMPLETED, ERROR, etc.).
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            message = self.get_message(conversation_id, message_id)
            status = message.get("status")
            
            if status in ["COMPLETED", "ERROR", "FAILED"]:
                return message
                
            time.sleep(poll_interval)
            
        raise TimeoutError(f"Message processing timed out after {timeout} seconds")

    def get_space(self, space_id: str) -> dict:
        """Get details of a specific Genie space."""
        response = self.client.genie.get_space(space_id=space_id)
        return response.as_dict()
    
    def list_conversations(self, space_id: str) -> dict:
        """Get conversations list of a specific Genie space."""
        response = self.client.genie.list_conversations(space_id=space_id)
        return response.as_dict()
    
    def get_conversation_messages(self, space_id: str, conversation_id: str) -> dict:
        """Get conversation messages of a specific Genie space."""
        response = self.client.genie.list_conversation_messages(space_id=space_id, conversation_id=conversation_id)
        return response.as_dict()
    
    def send_feedback(self, space_id: str, conversation_id: str, message_id: str, rating) -> dict:
        """Send feedback for a specific message within a Genie space."""
        response = self.client.genie.send_message_feedback(space_id=space_id, 
                                                            conversation_id=conversation_id, 
                                                            message_id=message_id, 
                                                            rating=rating)
        return response.as_dict()
    
    def delete_conversation(self, space_id: str, conversation_id: str) -> dict:
        """Delete conversation within a Genie space."""
        response = self.client.genie.delete_conversation(space_id=space_id, 
                                                         conversation_id=conversation_id)
        return response.as_dict()
    
def process_genie_response(client, conversation_id, message_id, complete_message) -> Tuple[Union[str, pd.DataFrame], Optional[str]]:
    """
    Process the response from Genie
    """
    # Check attachments first
    attachments = complete_message.get("attachments", [])
    for attachment in attachments:
        attachment_id = attachment.get("attachment_id")
        
        # If there's text content in the attachment, return it
        if "text" in attachment and "content" in attachment["text"]:
            return attachment["text"]["content"], None
        
        # If there's a query, get the result
        elif "query" in attachment:
            query_text = attachment.get("query", {}).get("query", "")
            query_result = client.get_query_result(conversation_id, message_id, attachment_id)
           
            data_array = query_result.get('data_array', [])
            schema = query_result.get('schema', {})
            columns = [col.get('name') for col in schema.get('columns', [])]
            
            # If we have data, return as DataFrame
            if data_array:
                # If no columns from schema, create generic ones
                if not columns and data_array and len(data_array) > 0:
                    columns = [f"column_{i}" for i in range(len(data_array[0]))]
                
                df = pd.DataFrame(data_array, columns=columns)
                return df, query_text
    
    # If no attachments or no data in attachments, return text content
    if 'content' in complete_message:
        return complete_message.get('content', ''), None
    
    return "No response available", None
    
def start_new_conversation(question: str, token: str, space_id: str, attachment: bytes = None, filename: str = None) -> Tuple[str, Union[str, pd.DataFrame], Optional[str]]:
    """
    Start a new conversation with Genie, optionally including an attachment.
    """
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    
    try:
        # Start a new conversation
        response = client.start_conversation(question)
        conversation_id = response["conversation_id"]
        message_id = response["message_id"]

        # If an attachment is provided, upload it
        if attachment and filename:
            client.upload_message_attachment(conversation_id, message_id, attachment, filename)
        
        # Wait for the message to complete
        complete_message = client.wait_for_message_completion(conversation_id, message_id)
        
        # Process the response
        result, query_text = process_genie_response(client, conversation_id, message_id, complete_message)
        
        return conversation_id, result, query_text, message_id
        
    except Exception as e:
        return None, f"Sorry, an error occurred: {str(e)}. Please try again.", None

def continue_conversation(conversation_id: str, question: str, token: str, space_id: str, attachment: bytes = None, filename: str = None) -> Tuple[Union[str, pd.DataFrame], Optional[str]]:
    """
    Send a follow-up message in an existing conversation.
    """
    logger.info(f"Continuing conversation {conversation_id} with question: {question[:30]}...")
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    
    try:
        # Send follow-up message in existing conversation
        response = client.send_message(conversation_id, question)
        message_id = response["message_id"]

        # If an attachment is provided, upload it
        if attachment and filename:
            client.upload_message_attachment(conversation_id, message_id, attachment, filename)
        
        # Wait for the message to complete
        complete_message = client.wait_for_message_completion(conversation_id, message_id)
        
        # Process the response
        result, query_text = process_genie_response(client, conversation_id, message_id, complete_message)
        
        return result, query_text, message_id
        
    except Exception as e:
        # Handle specific errors
        if "429" in str(e) or "Too Many Requests" in str(e):
            return "Sorry, the system is currently experiencing high demand. Please try again in a few moments.", None
        elif "Conversation not found" in str(e):
            return "Sorry, the previous conversation has expired. Please try your query again to start a new conversation.", None
        else:
            logger.error(f"Error continuing conversation: {str(e)}")
            return f"Sorry, an error occurred: {str(e)}", None
        
def list_conversations(token: str, space_id: str):
    """
    List conversations in a specific Genie space.
    """
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    
    try:
        response = client.list_conversations(space_id)
        all_conversations = response.get("conversations", [])
        return all_conversations
    except Exception as e:
        logger.error(f"Error listing conversations: {str(e)}")
        return []
    
def get_conversation_messages(token: str, space_id: str, conversation_id: str):
    """
    List conversations in a specific Genie space.
    """
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    
    try:
        response = client.get_conversation_messages(space_id, conversation_id)
        all_messages = response.get("messages", [])
        return all_messages
    except Exception as e:
        logger.error(f"Error listing conversations: {str(e)}")
        return []
    
def send_message_feedback(token: str, space_id: str, conversation_id: str, message_id: str, rating) -> dict:
    """
    Send message feedback for a specific message in a conversation.
    """
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )

    try:
        response = client.send_feedback(space_id, conversation_id, message_id, rating)
        return response
    except Exception as e:
        logger.error(f"Error getting feedback: {str(e)}")
        return {"error": str(e)}
    
def delete_conversation(token: str, space_id: str, conversation_id: str):
    """
    Delete conversation in a specific Genie space.
    """
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    
    try:
        response = client.delete_conversation(space_id, conversation_id)
        return response
    except Exception as e:
        logger.error(f"Error listing conversations: {str(e)}")
        return {"error": str(e)}