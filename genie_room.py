import pandas as pd
import os
import json
import requests
from io import BytesIO
from typing import Optional, Union, Tuple
import time
from datetime import datetime
import logging
from dotenv import load_dotenv
from databricks import sql
from modules import OfflineQueue, GenieClient
from databricks.sdk.service.sql import Disposition, Format

# Configure logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")

# Create Queue object
offline_queue = OfflineQueue()

#################
### Functions ###
#################

def process_genie_response(client, conversation_id, message_id, complete_message) -> Tuple[Union[str, pd.DataFrame], Optional[str]]:
    """Process the response from Genie"""
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
    
def start_new_conversation(question: str, token: str, space_id: str, http_path: str, catalog: str, schema: str, attachment: bytes = None, filename: str = None) -> Tuple[str, Union[str, pd.DataFrame], Optional[str]]:
    """Start a new conversation with Genie, optionally including an attachment."""
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    queue = offline_queue
    
    try:
        # Start a new conversation
        response = client.start_conversation(question)
        space_id = response["space_id"]
        conversation_id = response["conversation_id"]
        message_id = response["message_id"]
        user_id = response["user_id"]
        chat_title = response["chat_title"]
        created_timestamp = response["created_timestamp"]
        assistant_description = response["assistant_description"]

        logging.info(f"Started new conversation {conversation_id} in Genie.")

        # If an attachment is provided, upload it
        if attachment and filename:
            client.upload_message_attachment(conversation_id, message_id, attachment, filename)
            logging.info(f"Uploaded attachment {filename} to conversation {conversation_id}.")
        
        # Wait for the message to complete
        complete_message = client.wait_for_message_completion(conversation_id, message_id)
        
        # Process the response
        result, query_text = process_genie_response(client, conversation_id, message_id, complete_message)

        # Persist conversation and messages to database
        try:
            with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=http_path,
            access_token=token
            ) as conn:
                cursor = conn.cursor()

                # Generate friendly conversation title
                cursor.execute(f"""
                                SELECT AI_SUMMARIZE(?, 5) AS summarized_title
                                """, (chat_title,))
                ai_title = cursor.fetchone()[0]

                # Save conversation
                cursor.execute(f"""
                                INSERT INTO {catalog}.{schema}.conversations 
                                (space_id, conversation_id, user_id, chat_title, ai_title, created_timestamp)
                                VALUES (?, ?, ?, ?, ?, ?)
                                """, (space_id, conversation_id, user_id, chat_title, ai_title, datetime.fromisoformat(created_timestamp)))

                # Save messages
                cursor.execute(f"""
                                INSERT INTO {catalog}.{schema}.messages 
                                (message_id, conversation_id, space_id, user_id, prompt, completion, user_attachment, assistant_attachment, created_timestamp, rating, sql_run_version)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (message_id, conversation_id, space_id, user_id, question, assistant_description, filename, query_text, datetime.fromisoformat(created_timestamp), None, 1))

            logging.info(f"Persisted conversation {conversation_id} and initial messages.")

        except Exception as db_err:
            logging.warning(f"Error persisting conversation {conversation_id}: {str(db_err)} — Falling back to offline queue.")

            # Queue the data for later insertion
            queue.enqueue({
                "space_id": space_id,
                "conversation_id": conversation_id,
                "user_id": user_id,
                "chat_title": chat_title,
                "created_timestamp": created_timestamp,
                "message_id": message_id,
                "prompt": question,
                "completion": assistant_description,
                "user_attachment": filename,
                "assistant_attachment": query_text,
                "operation": "insert_new_conversation"
            })

        finally:
            try:
                cursor.close()
                conn.close()
                logging.info("Closed Databricks SQL connection.")
            except Exception as close_err:
                logging.warning(f"Error closing connection: {str(close_err)}")

        return conversation_id, result, query_text, message_id, assistant_description, ai_title
        
    except Exception as e:
        logging.error(f"Error starting new conversation: {str(e)}")
        return None, f"Sorry, an error occurred: {str(e)}. Please try again.", None, None, None, None

def continue_conversation(conversation_id: str, question: str, token: str, space_id: str, http_path: str, catalog: str, schema: str, attachment: bytes = None, filename: str = None) -> Tuple[Union[str, pd.DataFrame], Optional[str]]:
    """Send a follow-up message in an existing conversation."""
    logger.info(f"Continuing conversation {conversation_id} with question: {question[:30]}...")
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    queue = offline_queue
    
    try:
        # Send follow-up message in existing conversation
        response = client.send_message(conversation_id, question)
        message_id = response["message_id"]
        user_id = response["user_id"]
        created_timestamp = response["created_timestamp"]
        assistant_description = response["assistant_description"]

        # If an attachment is provided, upload it
        if attachment and filename:
            client.upload_message_attachment(conversation_id, message_id, attachment, filename)
        
        # Wait for the message to complete
        complete_message = client.wait_for_message_completion(conversation_id, message_id)
        
        # Process the response
        result, query_text = process_genie_response(client, conversation_id, message_id, complete_message)

        # Persist messages to database
        try:
            with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=http_path,
            access_token=token
            ) as conn:
                cursor = conn.cursor()

                # Save messages
                cursor.execute(f"""
                                INSERT INTO {catalog}.{schema}.messages 
                                (message_id, conversation_id, space_id, user_id, prompt, completion, user_attachment, assistant_attachment, created_timestamp, rating, sql_run_version)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (message_id, conversation_id, space_id, user_id, question, assistant_description, filename, query_text, datetime.fromisoformat(created_timestamp), None, 1))

            logging.info(f"Persisted follow-up message {message_id}.")

        except Exception as db_err:
            logging.warning(f"Error persisting follow-up message {message_id}: {str(db_err)} — Falling back to offline queue.")

            # Queue the data for later insertion
            queue.enqueue({
                "message_id": message_id,
                "conversation_id": conversation_id,
                "space_id": space_id,
                "user_id": user_id,
                "prompt": question,
                "completion": assistant_description,
                "user_attachment": filename,
                "assistant_attachment": query_text,
                "created_timestamp": created_timestamp,
                "operation": "insert_message"
            })

        finally:
            try:
                cursor.close()
                conn.close()
                logging.info("Closed Databricks SQL connection.")
            except Exception as close_err:
                logging.warning(f"Error closing connection: {str(close_err)}")
        
        return result, query_text, message_id, assistant_description
        
    except Exception as e:
        # Handle specific errors
        if "429" in str(e) or "Too Many Requests" in str(e):
            return "Sorry, the system is currently experiencing high demand. Please try again in a few moments.", None
        elif "Conversation not found" in str(e):
            return "Sorry, the previous conversation has expired. Please try your query again to start a new conversation.", None
        else:
            logger.error(f"Error continuing conversation: {str(e)}")
            return f"Sorry, an error occurred: {str(e)}", None, None, None
    
def send_message_feedback(token: str, space_id: str, conversation_id: str, message_id: str, rating, http_path: str, catalog: str, schema: str):
    """Send message feedback for a specific message in a conversation."""
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    queue = offline_queue

    try:
        client.send_feedback(space_id, conversation_id, message_id, rating)
        logger.info(f"Sent rating {str(rating)} for message {message_id} in conversation {conversation_id}.")
    
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=http_path,
            access_token=token
        ) as conn:
            cursor = conn.cursor()

            cursor.execute(f"""
                            UPDATE {catalog}.{schema}.messages
                            SET rating = ?
                            WHERE message_id = ?
                            """, (str(rating).split('.')[-1], message_id))
            
            logger.info(f"Message {message_id} rating in conversation {conversation_id} updated in Database.")

    except Exception as e:
        logger.error(f"Error getting feedback: {str(e)} — Falling back to offline queue.")

        # Queue the data for later insertion
        queue.enqueue({
                "message_id": message_id,
                "rating": str(rating),
                "operation": "update_rating"
            })
    
    finally:
        try:
            cursor.close()
            conn.close()
            logging.info("Closed Databricks SQL connection successfully.")
        except Exception as close_err:
            logging.warning(f"Error closing connection: {str(close_err)}")
    
def delete_conversation(token: str, space_id: str, conversation_id: str, http_path: str, catalog: str, schema: str):
    """Delete conversation in a specific Genie space."""
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    queue = offline_queue
    
    try:
        client.delete_conversation(space_id, conversation_id)
        logger.info(f"Deleted conversation {conversation_id} in Genie.")

        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=http_path,
            access_token=token
        ) as conn:
            cursor = conn.cursor()

            # Delete messages from DB
            cursor.execute(f"""
                            DELETE FROM {catalog}.{schema}.messages 
                            WHERE conversation_id = ? 
                            """, (conversation_id,))

            # Delete conversation from DB
            cursor.execute(f"""
                            DELETE FROM {catalog}.{schema}.conversations 
                            WHERE conversation_id = ? 
                            """, (conversation_id,))
        
        logger.info(f"Deleted conversation {conversation_id} and its messages from Database.")
    
    except Exception as e:
        logger.error(f"Error deleting conversation {conversation_id}: {str(e)} — Falling back to offline queue.")
    
        # Queue the data for later insertion
        queue.enqueue({
                "conversation_id": conversation_id,
                "operation": "delete"
            })
    
    finally:
        try:
            cursor.close()
            conn.close()
            logging.info("Closed Databricks SQL connection successfully.")
        except Exception as close_err:
            logging.warning(f"Error closing connection: {str(close_err)}")

def execute_sql_with_polling(space_id: str, token: str, http_path: str, catalog: str, schema: str, warehouse_id: str, message_id: str, sql_text: str, use_external: bool, poll_interval=2, timeout=300):
    """Executes SQL using statement_execution, waits, gets all chunks and returns a DataFrame."""
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )

    if use_external:
        disposition = Disposition.EXTERNAL_LINKS
        fmt = Format.CSV
    else:
        disposition = Disposition.INLINE
        fmt = Format.JSON_ARRAY

    # Execute statement
    statement_id = client.execute_statement(warehouse_id, sql_text, disposition, fmt)

    # Polling
    start = time.time()
    while True:
        stmt = client.get_statement(statement_id)
        state = stmt.status.state.value

        if state in ["SUCCEEDED", "FAILED", "CANCELED", "CLOSED"]:
            break

        if time.time() - start > timeout:
            raise TimeoutError(f"Statement {statement_id} timed out.")

        time.sleep(poll_interval)

    # Update sql_run_version if succeeded
    if state == "SUCCEEDED":
        try:
            with sql.connect(
                server_hostname=DATABRICKS_HOST,
                http_path=http_path,
                access_token=token
            ) as conn:
                cursor = conn.cursor()

                # Update sql_run_version in DB
                cursor.execute(f"""
                                UPDATE {catalog}.{schema}.messages
                                SET sql_run_version = sql_run_version + 1
                                WHERE message_id = ?
                                """, (message_id,))
        
            logger.info(f"Update message {message_id} in Database.")
    
        except Exception as e:
            logger.error(f"Error updating message {message_id}: {str(e)}.")

        finally:
            try:
                cursor.close()
                conn.close()
                logging.info("Closed Databricks SQL connection successfully.")
            except Exception as close_err:
                logging.warning(f"Error closing connection: {str(close_err)}")

    logger.info(f"Statement result format: {fmt}")

    # Get results based on response disposition and format
    if fmt == Format.JSON_ARRAY:

        chunks = []
        idx = 0

        while True:
            chunk = client.get_chunk(statement_id, idx)

            if not getattr(chunk, "data_array", None):
                break

            chunks.extend(chunk.data_array)
            next_idx = getattr(chunk, "next_chunk_index", None)
            if next_idx is None:
                break
            idx = next_idx

        columns = [col.name for col in stmt.manifest.schema.columns]
        df = pd.DataFrame(chunks, columns=columns)

    elif fmt == Format.CSV:
        dfs = []

        for link in stmt.result.external_links:
            url = link.external_link

            resp = requests.get(url)
            resp.raise_for_status()

            # CSV to DataFrame
            df_part = pd.read_csv(BytesIO(resp.content))
            dfs.append(df_part)

        df = pd.concat(dfs, ignore_index=True)
    
    return df

def current_user(space_id: str, token: str):
    """Get the current authenticated user information"""
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    user = client.current_user()
    return user

def semantic_search(space_id: str, token: str, http_path: str, catalog: str, schema: str, query_text: str):
    """Query vector search index for similar user questions (see vector_resources.py for further details)."""
    import datetime
    client = GenieClient(
        host=DATABRICKS_HOST,
        space_id=space_id,
        token=token
    )
    queue = offline_queue

    try:
        user_info = client.current_user()
        columns = ["prompt", "completion", "assistant_attachment", "message_id", "user_id"]
        filters = json.dumps({"space_id": space_id})
        results = client.similarity_search("messages_user_questions_index",
                                           catalog,
                                           schema,
                                           columns,
	                                       3,
                                           query_text,
                                           filters)
        results_str = [str(row[2]) for row in results]
        message_ids = [str(row[3]) for row in results]
        score = [float(row[-1]) for row in results]
        results_str_array = "ARRAY(" + ", ".join(f"'{x}'" for x in results_str) + ")"
        message_ids_array = "ARRAY(" + ", ".join(f"'{x}'" for x in message_ids) + ")"
        score_array = "ARRAY(" + ", ".join(str(x) for x in score) + ")"
        timestamp = datetime.datetime.now().timestamp()
        
        # Persist search to database
        try:
            with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=http_path,
            access_token=token
                ) as conn:
                cursor = conn.cursor()

                # Save messages
                cursor.execute(f"""
                                INSERT INTO {catalog}.{schema}.similarity_search 
                                (space_id, message_id, user_id, user_name, user_email, prompt, completion, score, created_timestamp)
                                VALUES (?, {message_ids_array}, ?, ?, ?, ?, {results_str_array}, {score_array}, ?)
                                """, (space_id, user_info["user_id"], user_info["user_name"], user_info["email"], query_text, timestamp))
                
            logging.info(f"Persisted semantic search successfully.")

        except Exception as db_err:
            logging.warning(f"Error persisting semantic search results: {str(db_err)} — Falling back to offline queue.")

            # Queue the data for later insertion
            queue.enqueue({
                "space_id": space_id,
                "message_id": message_ids_array,
                "user_id": user_info["user_id"],
                "user_name": user_info["user_name"],
                "user_email": user_info["email"],
                "prompt": query_text,
                "completion": results_str_array,
                "score": score_array,
                "created_timestamp": timestamp,
                "operation": "insert_similar_search"
            })

        finally:
            try:
                cursor.close()
                conn.close()
                logging.info("Closed Databricks SQL connection.")
            except Exception as close_err:
                logging.warning(f"Error closing connection: {str(close_err)}")

        return results
    
    except Exception as e:
        logger.error(f"Error getting semantic search results: {str(e)}")
        return {"error": str(e)}