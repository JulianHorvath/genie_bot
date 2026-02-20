# pytest -q --> runs all tests
# pytest -q tests/test_genie_client.py --> runs all tests in this file
# pytest -q tests/test_genie_client.py::test_start_and_send_and_upload --> run specific test in a specific file

from unittest.mock import patch, MagicMock
from types import SimpleNamespace
import sys
import os

# Ensure parent directory matches genie_room module location
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Test GenieClient methods isolating WorkspaceClient with mock
@patch("modules.WorkspaceClient") #genie_room.WorkspaceClient
def test_start_and_send_and_upload(MockWorkspace):
    # Arrange: build mock for workspace/genie
    mock_ws = MagicMock()
    MockWorkspace.return_value = mock_ws

    from modules import GenieClient
    gc = GenieClient(host="host", space_id="space-1", token="tok")

    # start_conversation mock
    start_result = SimpleNamespace(conversation_id="conv-1", 
                                   message_id="msg-1",
                                   user_id="user-1",
                                   created_timestamp=1_700_000_000_000,
                                   attachments=[])

    mock_ws.genie.start_conversation.return_value.result.return_value = start_result
    out = gc.start_conversation("Hi")
    assert out["conversation_id"] == "conv-1"
    assert out["message_id"] == "msg-1"

    # send_message mock
    send_result = SimpleNamespace(
        message_id="msg-2",
        user_id="user-1",
        created_timestamp=1_700_000_000_000,
        attachments=[]
        )
    mock_ws.genie.create_message.return_value.result.return_value = send_result
    out2 = gc.send_message("conv-1", "Hello")
    assert out2["message_id"] == "msg-2"

    # upload_message_attachment should call the internal API with proper parameters
    gc.upload_message_attachment("conv-1", "msg-1", b"content", "file.csv")
    mock_ws.genie.upload_message_attachment.assert_called_once_with(
        space_id=gc.space_id,
        conversation_id="conv-1",
        message_id="msg-1",
        file=b"content",
        name="file.csv"
    )

@patch("modules.WorkspaceClient")
def test_get_query_result_parses_data_and_schema(MockWorkspace):
    mock_ws = MagicMock()
    MockWorkspace.return_value = mock_ws

    from modules import GenieClient
    gc = GenieClient(host="h", space_id="s", token="t")

    # Built nested response that waits for get_query_result
    mock_response = SimpleNamespace(
        statement_response=SimpleNamespace(
            result=SimpleNamespace(data_array=[["v1", 123]]),
            manifest=SimpleNamespace(schema=SimpleNamespace(as_dict=lambda: {"columns": [{"name": "c1"}, {"name": "c2"}]}))
        )
    )
    mock_ws.genie.get_message_attachment_query_result.return_value = mock_response

    res = gc.get_query_result("conv", "msg", "att1")
    assert res["data_array"] == [["v1", 123]]
    assert "columns" in res["schema"]

@patch("modules.WorkspaceClient")
def test_execute_query_returns_dict(MockWorkspace):
    mock_ws = MagicMock()
    MockWorkspace.return_value = mock_ws

    from modules import GenieClient
    gc = GenieClient(host="h", space_id="s", token="t")

    mock_exec_response = SimpleNamespace(as_dict=lambda: {"ok": True})
    mock_ws.genie.execute_message_attachment_query.return_value = mock_exec_response

    out = gc.execute_query("conv", "msg", "att")
    assert isinstance(out, dict)
    assert out["ok"] is True

@patch("modules.WorkspaceClient")
def test_wait_for_message_completion_polls_until_complete(MockWorkspace, monkeypatch):
    mock_ws = MagicMock()
    MockWorkspace.return_value = mock_ws

    from modules import GenieClient
    gc = GenieClient(host="h", space_id="s", token="t")

    # Replace get_message from client for a function returning intermediate states
    responses = [
        {"status": "PROCESSING"},
        {"status": "COMPLETED", "attachments": []}
    ]
    gc.get_message = MagicMock(side_effect=responses)

    # Patch time.sleep in the module to avoid delays during test
    monkeypatch.setattr("genie_room.time.sleep", lambda _ : None)

    finished = gc.wait_for_message_completion("conv", "msg", timeout=5, poll_interval=0)
    assert finished.get("status") == "COMPLETED"
