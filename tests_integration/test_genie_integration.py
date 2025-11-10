# pytest -q -m integration tests_integration/

import os
import sys
import pytest
import pandas as pd

# Ensure parent directory matches genie_room module location
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from genie_room import start_new_conversation, continue_conversation

# These tests require environment variables to be set
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
GENIE_SPACE = os.getenv("GENIE_SPACE")

@pytest.mark.integration
def test_start_new_conversation_real():

    # Real flow: ask something to start a conversation with Genie
    conv_id, result, query_text = start_new_conversation(
        "Show 3 rows from any table accesible on this Genie Space",
        DATABRICKS_TOKEN,
        GENIE_SPACE
    )

    assert conv_id is not None
    assert result is not None
    assert isinstance(result, (str, pd.DataFrame)) # Can return text or DF (check both)

@pytest.mark.integration
def test_continue_conversation_real():
    # Start conversation
    conv_id, result, _ = start_new_conversation(
        "What is the region with more connected vehicles?",
        DATABRICKS_TOKEN,
        GENIE_SPACE
    )

    # Continue real conversation
    result, query_text = continue_conversation(
        conv_id,
        "Now return region with less connected vehicles, excluding null",
        DATABRICKS_TOKEN,
        GENIE_SPACE
    )

    assert result is not None

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
