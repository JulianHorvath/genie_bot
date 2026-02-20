# pytest -q -m integration tests_integration/

import os
import sys
import pytest
import pandas as pd
import uuid
from datetime import datetime
from databricks import sql
from dotenv import load_dotenv

# Ensure parent directory matches genie_room module location
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from genie_room import start_new_conversation, continue_conversation

# These tests require environment variables to be set
load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
HTTP_PATH = os.getenv("HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
GENIE_SPACE = os.getenv("GENIE_SPACE")
CATALOG = os.environ.get("CATALOG")
SCHEMA = os.environ.get("SCHEMA")

@pytest.mark.integration
def test_conversation_table_connectivity_and_insert():

    test_conversation_id = str(uuid.uuid4())
    test_user_id = "999999"
    test_title = "integration test"
    test_ts = datetime.fromtimestamp(1234567890)

    conn = sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )
    cursor = conn.cursor()

    try:
        # Insert test
        cursor.execute(
            f"""
            INSERT INTO {CATALOG}.{SCHEMA}.conversations_test
            (test_conversation_id, test_user_id, test_title, test_ts)
            VALUES (?, ?, ?, ?)
            """,
            (test_conversation_id, test_user_id, test_title, test_ts)
        )

        # Read test
        cursor.execute(
            f"""
            SELECT test_conversation_id, test_user_id, test_title
            FROM {CATALOG}.{SCHEMA}.conversations_test
            WHERE test_conversation_id = ?
            """,
            (test_conversation_id,)
        )
        row = cursor.fetchone()

        # Assertions
        assert row is not None
        assert row.test_conversation_id == test_conversation_id
        assert row.test_user_id == test_user_id
        assert row.test_title == test_title

    finally:
        # Cleanup
        cursor.execute(
            f"""
            DELETE FROM {CATALOG}.{SCHEMA}.conversations_test
            WHERE test_conversation_id = ?
            """,
            (test_conversation_id,)
        )

        cursor.close()
        conn.close()

@pytest.mark.integration
def test_start_new_conversation_real():

    # Real flow: ask something to start a conversation with Genie
    conv_id, result, query_text, _, _ = start_new_conversation(
        "Show 3 rows from any table accesible on this Genie Space",
        DATABRICKS_TOKEN,
        GENIE_SPACE,
        HTTP_PATH,
        CATALOG,
        SCHEMA
    )

    assert conv_id is not None
    assert result is not None
    if isinstance(result, pd.DataFrame):
        assert not result.empty
    if query_text:
        assert "SELECT" in query_text.upper()
    #assert isinstance(result, (str, pd.DataFrame)) # Can return text or DF (check both)



@pytest.mark.integration
def test_continue_conversation_real():
    # Start conversation
    conv_id, result, query_text, _, _ = start_new_conversation(
        "What is the region with more connected vehicles?",
        DATABRICKS_TOKEN,
        GENIE_SPACE,
        HTTP_PATH,
        CATALOG,
        SCHEMA
    )

    # Continue real conversation
    result, query_text, _, _ = continue_conversation(
        conv_id,
        "Now return region with less connected vehicles, excluding null",
        DATABRICKS_TOKEN,
        GENIE_SPACE,
        HTTP_PATH,
        CATALOG,
        SCHEMA
    )

    assert result is not None
    if isinstance(result, pd.DataFrame):
        assert not result.empty
    if query_text:
        assert "SELECT" in query_text.upper()

"""
@pytest.mark.integration
def test_upload_attachment_real(tmp_path):
    # Create a sample .csv file to upload
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("col1,col2\n1,2\n3,4")

    with open(csv_file, "rb") as f:
        attachment_bytes = f.read()

    conv_id, result, query_text = start_new_conversation(
        "Load this file and show me its data",
        DATABRICKS_TOKEN,
        GENIE_SPACE,
        attachment=attachment_bytes,
        filename="sample.csv"
    )

    assert conv_id is not None
    assert result is not None
"""