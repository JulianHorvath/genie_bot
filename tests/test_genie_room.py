# pytest -q --> runs all tests
# pytest -q tests/test_genie_room.py --> runs all tests in this file
# pytest -q tests/test_genie_room.py::test_start_new_conversation_uploads_attachment_and_returns_dataframe # run specific test in a specific file

from unittest.mock import patch
import pandas as pd
import sys
import os

# Ensure parent directory matches genie_room module location
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Test start_new_conversation to assert:
# - Invokes GenieClient
# - Upload attachment if exists
# - Return processed result by process_genie_response

def test_start_new_conversation_uploads_attachment_and_returns_dataframe():
    with patch("genie_room.GenieClient") as MockGenieClient:
        mock_client = MockGenieClient.return_value

        # start_conversation returns ids
        mock_client.start_conversation.return_value = {"conversation_id": "conv-1", "message_id": "msg-1"}
        # wait_for_message_completion returns an object
        mock_client.wait_for_message_completion.return_value = {"status": "COMPLETED"}

        # Patch process_genie_response to control output
        with patch("genie_room.process_genie_response", return_value=(pd.DataFrame([[1, 2]], columns=["a", "b"]), "SELECT 1")) as mock_proc:
            from genie_room import start_new_conversation
            conv_id, result, query_text = start_new_conversation("consulta", "token", "space", attachment=b"datos", filename="f.csv")

            # assertions
            assert conv_id == "conv-1"
            assert isinstance(result, pd.DataFrame)
            assert query_text == "SELECT 1"
            # upload_message_attachment should be passed correct parameters
            mock_client.upload_message_attachment.assert_called_once_with("conv-1", "msg-1", b"datos", "f.csv")

def test_continue_conversation_with_attachment_calls_upload_and_returns_result():
    with patch("genie_room.GenieClient") as MockGenieClient:
        mock_client = MockGenieClient.return_value
        # send_message returns message_id
        mock_client.send_message.return_value = {"message_id": "msg-2"}
        mock_client.wait_for_message_completion.return_value = {"status": "COMPLETED"}

        with patch("genie_room.process_genie_response", return_value=("text response", None)) as mock_proc:
            from genie_room import continue_conversation
            result, query_text = continue_conversation("conv-1", "new question", "token", "space", attachment=b"bits", filename="data.csv")

            assert isinstance(result, str)
            mock_client.upload_message_attachment.assert_called_once_with("conv-1", "msg-2", b"bits", "data.csv")