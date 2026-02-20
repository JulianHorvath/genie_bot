# Configure this script to reprocess failed SQL operations from the offline queue.
# It can be triggered manually (local) or by a Databricks Job (prod).
# Uncomment the last lines to run the reprocessing.

import logging
import time
from datetime import datetime
import os
from dotenv import load_dotenv
from databricks import sql
from modules import OfflineQueue

# Load environment variables
load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
HTTP_PATH = os.environ.get("HTTP_PATH")
CATALOG = os.environ.get("CATALOG")
SCHEMA = os.environ.get("SCHEMA")
RETRY_DELAY = 10  # seconds between items
MAX_RETRIES = 3 # max retries per item

def reprocess_offline_queue(queue, host, token, http_path, catalog, schema):
    """
    Reprocess failed SQL operations from the offline queue.
    Designed to be triggered manually (local) or by a Databricks Job (prod).
    """
    logging.info("Starting offline queue reprocessing...")

    while True:
        item = queue.dequeue()
        if not item:
            logging.info("Queue is empty. Nothing to reprocess.")
            break

        op = item.get("operation")
        conversation_id = item.get("conversation_id")
        message_id = item.get("message_id")
        search_timestamp = item.get("created_timestamp")

        try:
            with sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=token
            ) as conn:
                cursor = conn.cursor()

                ### Insert operations ###
                # New conversation + initial messages
                if op == "insert_new_conversation":
                    
                    # Check duplicates for conversations to insert
                    cursor.execute(
                        f"SELECT 1 FROM {catalog}.{schema}.conversations WHERE conversation_id = ?", 
                        (conversation_id,)
                    )
                    exists_convs = cursor.fetchone()

                    if exists_convs:
                        logging.info(f"[SKIP] Conversation {conversation_id} already exists.")
                        continue

                    # Perform insert for conversations queued
                    cursor.execute(f"""
                                    INSERT INTO {catalog}.{schema}.conversations
                                    (space_id, conversation_id, user_id, chat_title, ai_title, created_timestamp)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    """, (item["space_id"], item["conversation_id"], item["user_id"], item["chat_title"], None, datetime.fromisoformat(item["created_timestamp"])))
                    logging.info(f"[OK] Insert retried: {conversation_id}")

                    # Perform insert for initial messages queued
                    cursor.execute(f"""
                                    INSERT INTO {catalog}.{schema}.messages
                                    (message_id, conversation_id, space_id, user_id, prompt, completion, user_attachment, assistant_attachment, created_timestamp, rating, sql_run_version)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (item["message_id"], item["conversation_id"], item["space_id"], item["user_id"], item["prompt"], item["completion"], item["user_attachment"], item["assistant_attachment"], datetime.fromisoformat(item["created_timestamp"]), None, 1))
                    logging.info(f"[OK] Insert retried: {message_id}")

                # Follow up messages
                elif op == "insert_message":
                    # Check duplicates for messages to insert
                    cursor.execute(
                        f"SELECT 1 FROM {catalog}.{schema}.messages WHERE message_id = ?",
                        (message_id,)
                    )
                    exists_mssgs = cursor.fetchone()

                    if exists_mssgs:
                        logging.info(f"[SKIP] Message {message_id} already exists.")
                        continue

                    # Perform insert for follow up messages queued
                    cursor.execute(f"""
                                    INSERT INTO {catalog}.{schema}.messages
                                    (message_id, conversation_id, space_id, user_id, prompt, completion, user_attachment, assistant_attachment, created_timestamp, rating, sql_run_version)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (item["message_id"], item["conversation_id"], item["space_id"], item["user_id"], item["prompt"], item["completion"], item["user_attachment"], item["assistant_attachment"], datetime.fromisoformat(item["created_timestamp"]), None, 1))
                    logging.info(f"[OK] Insert retried: {message_id}")

                # Similarity search
                elif op == "insert_similar_search":
                    # Check duplicates for messages to insert
                    cursor.execute(
                        f"SELECT 1 FROM {catalog}.{schema}.similarity_search WHERE created_timestamp = ?",
                        (search_timestamp,)
                    )
                    exists_search = cursor.fetchone()

                    if exists_search:
                        logging.info(f"[SKIP] Search at timestamp {search_timestamp} already exists.")
                        continue

                    # Perform insert for messages queued
                    cursor.execute(f"""
                                    INSERT INTO {catalog}.{schema}.similarity_search
                                    (space_id, message_id, user_id, user_name, user_email, prompt, completion, score, created_timestamp)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """, (item["space_id"], item["message_id"], item["user_id"], item["user_name"], item["user_email"], item["prompt"], item["completion"], item["score"], item["created_timestamp"]))
                    logging.info(f"[OK] Insert retried: {search_timestamp}")

                ### Delete operations ###
                elif op == "delete":
                    # Check duplicates for conversations to delete
                    cursor.execute(
                        f"SELECT 1 FROM {catalog}.{schema}.conversations WHERE conversation_id = ?",
                        (conversation_id,)
                    )
                    exists_convers = cursor.fetchone()

                    if not exists_convers:
                        logging.info(f"[SKIP] Conversation {conversation_id} already deleted.")
                        continue

                    # Perform delete for messages queued (messages first to guarantee referential integrity)
                    cursor.execute(f"""
                                   DELETE FROM {catalog}.{schema}.messages
                                   WHERE conversation_id = ?
                                   """, (conversation_id,))
                    logging.info(f"[OK] Delete retried: conversation {conversation_id} messages")

                    # Perform delete for conversations queued
                    cursor.execute(f"""
                                   DELETE FROM {catalog}.{schema}.conversations
                                   WHERE conversation_id = ?
                                   """, (conversation_id,))
                    logging.info(f"[OK] Delete retried: {conversation_id}")

                ### Update rating ###
                elif op == "update_rating":
                    # Check existing rating for messages to update
                    cursor.execute(
                        f"SELECT rating FROM {catalog}.{schema}.messages WHERE message_id = ?",
                        (message_id,)
                    )
                    exists_rating = cursor.fetchone()

                    if exists_rating == item['rating']:
                        logging.info(f"[SKIP] Message {message_id} already rated.")
                        continue

                    # Perform update rating for messages queued
                    cursor.execute(f"""
                                   UPDATE {catalog}.{schema}.messages
                                   SET rating = ?
                                   WHERE message_id = ?
                                   """, (item['rating'], message_id))
                    logging.info(f"[OK] Update retried: message {message_id} rating to {item['rating']}")

        # Handle exceptions and retries
        except Exception as err:
            attempts = item.get("retries", 0)
            if attempts < MAX_RETRIES:
                item["retries"] = attempts + 1
                queue.enqueue(item)  # back to queue
                logging.warning(f"Retry {attempts+1}/{MAX_RETRIES} for item {item["id"]} — {err}")
            else:
                item["retries"] = 0 # reset retries for future reprocessing
                queue.enqueue(item)  # back to queue permanently
                logging.error(f"FAILED permanently: {item["id"]}: {err}")
            time.sleep(RETRY_DELAY)
            continue

        finally:
            try:
                cursor.close()
                conn.close()
                logging.info("Closed Databricks SQL connection after offline queue reprocessing.")
            except Exception as close_err:
                logging.warning(f"Error closing connection: {str(close_err)}")

    logging.info("Finished offline queue reprocessing.")

#queue = OfflineQueue()
#reprocess_offline_queue(queue, DATABRICKS_HOST, DATABRICKS_TOKEN, HTTP_PATH, CATALOG, SCHEMA)