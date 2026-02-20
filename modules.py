import os
import sqlite3
import time
from datetime import datetime
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from typing import Dict, Any

###############
### Classes ###
###############

# Class to handle offline queuing of failed inserts
class OfflineQueue:
    def __init__(self, dbfs_path: str = "/dbfs/tmp/genie_queue", sqlite_file: str = "fallback.db"):
        self.is_databricks = os.getenv("DATABRICKS_RUNTIME_VERSION") is not None
        self.dbfs_path = dbfs_path
        self.sqlite_file = sqlite_file

        if self.is_databricks:
            os.makedirs(dbfs_path, exist_ok=True)
        else:
            conn = sqlite3.connect(self.sqlite_file)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pending (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            conn.close()

    def enqueue(self, payload: dict):
        """Save failed inserts depending on environment. Priority: DBFS -> SQLite"""
        if self.is_databricks:
            fname = f"{self.dbfs_path}/{datetime.now().timestamp()}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(payload, f)
        else:
            conn = sqlite3.connect(self.sqlite_file)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO pending (payload) VALUES (?)",
                (json.dumps(payload),)
            )
            conn.commit()
            conn.close()

    def dequeue(self):
        """Retrieve and remove the oldest queued item. Priority: DBFS -> SQLite"""
        # Try DBFS backend 
        try:
            if os.path.exists(self.dbfs_path):
                with open(self.dbfs_path, "r") as f:
                    data = json.load(f)

                if data:
                    item = data.pop(0) # remove first item
                    with open(self.dbfs_path, "w") as f:
                        json.dump(data, f, indent=2)
                    return item
        except Exception:
            pass  # fallback to sqlite

        # Try SQLite backend
        try:
            conn = sqlite3.connect(self.sqlite_file)
            cursor = conn.cursor()

            cursor.execute("SELECT id, payload FROM pending ORDER BY id ASC LIMIT 1")
            row = cursor.fetchone()

            if not row:
                return None

            item_id, payload = row
            cursor.execute("DELETE FROM pending WHERE id = ?", (item_id,))
            conn.commit()
            conn.close()

            return json.loads(payload)

        except Exception:
            return None

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
        response = response.result()
        genie_description = None
        for attachment in response.attachments or []:
            if attachment.query and attachment.query.description:
                genie_description = attachment.query.description
                break
        response_dict = {
            "space_id": self.space_id,
            "conversation_id": str(response.conversation_id),
            "user_id": str(response.user_id),
            "chat_title": question,
            "assistant_description": str(genie_description),
            "created_timestamp": datetime.fromtimestamp(response.created_timestamp / 1000).isoformat(),
            "message_id": str(response.message_id)
        }
        return response_dict
    
    def send_message(self, conversation_id: str, message: str) -> Dict[str, Any]:
        """Send a follow-up message to an existing conversation"""
        response = self.client.genie.create_message(
            space_id=self.space_id,
            conversation_id=conversation_id,
            content=message
        )
        response = response.result()
        genie_description = None
        for attachment in response.attachments or []:
            if attachment.query and attachment.query.description:
                genie_description = attachment.query.description
                break
        response_dict = {
            "message_id": str(response.message_id),
            "user_id": str(response.user_id),
            "assistant_description": str(genie_description),
            "created_timestamp": datetime.fromtimestamp(response.created_timestamp / 1000).isoformat()
        }
        return response_dict
    
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
    
    def execute_query(self, conversation_id: str, message_id: str, attachment_id: str) -> Dict[str, Any]:
        """Execute a query using the attachment_id endpoint"""
        response = self.client.genie.execute_message_attachment_query(
            space_id=self.space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            attachment_id=attachment_id
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

    def wait_for_message_completion(self, conversation_id: str, message_id: str, timeout: int = 300, poll_interval: int = 2) -> Dict[str, Any]:
        """Wait for a message to reach a terminal state (COMPLETED, ERROR, etc.)."""
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
    
    def send_feedback(self, space_id: str, conversation_id: str, message_id: str, rating):
        """Send feedback for a specific message within a Genie space."""
        response = self.client.genie.send_message_feedback(space_id=space_id, 
                                                            conversation_id=conversation_id, 
                                                            message_id=message_id, 
                                                            rating=rating)
        return response
    
    def delete_conversation(self, space_id: str, conversation_id: str):
        """Delete conversation within a Genie space."""
        response = self.client.genie.delete_conversation(space_id=space_id, 
                                                         conversation_id=conversation_id)
        return response
    
    def execute_statement(self, warehouse_id: str, sql: str, disposition, format):
        """Executes a SQL statement and returns statement_id for polling."""
        statement = self.client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            row_limit=100000,
            wait_timeout="0s",
            disposition=disposition,
            format=format
        )
        return statement.statement_id
    
    def get_statement(self, statement_id: str):
        """Gets current state for the statement: PENDING, RUNNING, SUCCEEDED, FAILED, CANCELED, etc."""
        return self.client.statement_execution.get_statement(statement_id)
    
    def get_chunk(self, statement_id: str, chunk_index: int):
        """Returns a chunk from results."""
        return self.client.statement_execution.get_statement_result_chunk_n(
            statement_id=statement_id,
            chunk_index=chunk_index
        )
    
    def current_user(self) -> Dict[str, Any]:
        """Get the current authenticated user"""
        response = self.client.current_user.me()
        return {"user_id": str(response.id),
                "user_name": str(response.display_name),
                "email": str(response.user_name),
                "groups": [str(group.display) for group in response.groups]
                }
    
    def similarity_search(self, index: str, catalog: str, schema: str, columns: list, num_results: int, query_text: str, filters: str):
        """Query vector search index for similar user questions."""
        response = self.client.vector_search_indexes.query_index(index_name=f"{catalog}.{schema}.{index}",
                                                                 columns=columns,
                                                                 num_results=num_results,
                                                                 query_text=query_text,
                                                                 filters_json=filters)
        return response.result.data_array #[0][0] to access result content of first match