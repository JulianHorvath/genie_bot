import streamlit as st
from streamlit import cache_data
from genie_room import start_new_conversation, continue_conversation, delete_conversation
from dotenv import load_dotenv
import logging
import os
import pandas as pd

# Load environment variables
load_dotenv()

GENIE_SPACE = os.environ.get("GENIE_SPACE")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cached retrieval of conversations and messages
# Conversations
@st.cache_data(ttl=300, show_spinner="Fetching data from Genie ⏳")  # Cache valid for 5 minutes
def get_cached_conversations(token: str, space_id: str):
    """Retrieve and cache conversations list from Genie."""
    from genie_room import list_conversations
    response = list_conversations(token, space_id)
    # Order by created_timestamp descending
    return sorted(
        response,
        key=lambda c: c.get("created_timestamp", ""),
        reverse=True
        )

# Messages
@st.cache_data(ttl=300, show_spinner="Fetching data from Genie ⏳")  # Cache valid for 5 minutes
def get_cached_messages(token: str, space_id: str, conversation_id: str):
    """Retrieve and cache conversations messages from Genie."""
    from genie_room import get_conversation_messages
    response = get_conversation_messages(token, space_id, conversation_id)
    return response

# Callback function to send feedback
def send_feedback_callback(conversation_id: str, message_id: str, rating_str):
    """Sends feedback and registers rating in session_state."""
    from genie_room import send_message_feedback
    pat = st.session_state.get("Databricks PAT")
    if not pat:
        pass
    try:
        send_message_feedback(
            pat,
            GENIE_SPACE,
            conversation_id=conversation_id,
            message_id=message_id, 
            rating=rating_str
        )
        # Register feedback for visual persistence
        st.session_state[f"rating_{message_id}"] = rating_str # Stores 'positive' or 'negative'
        st.toast(f"✅ Feedback '{rating_str}' sent. Thanks!", icon="💬")  
    except Exception as e:
        logger.error(f"Feedback submission failed: {str(e)}")
        st.toast("⚠️ Error while sending feedback", icon="⚠️")

# Page configuration
st.set_page_config(
    page_title="<team_name> Bot powered by Genie",
    page_icon=":streamlit:",
    layout=None,
    initial_sidebar_state="expanded",
    menu_items={
        'About': "# This is a <team_name> product."
    }
)

# App title
st.title("Genie Bot 🤖")

# Initialize chat history (If no messages)
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None
if "messages" not in st.session_state:
    st.session_state.messages = []
if "show_examples" not in st.session_state:
    st.session_state.show_examples = True  # Show examples

# Reset conversation if new chat started
if "new_chat_started" in st.session_state and st.session_state.new_chat_started:
    st.session_state.new_chat_started = False
    st.session_state.conversation_id = None
    st.session_state.messages = []
    st.session_state.show_examples = True # Show examples in new chat

    # Clean all ratings stored when new chat starts
    keys_to_delete = [k for k in st.session_state.keys() if k.startswith("rating_")]
    for k in keys_to_delete:
        del st.session_state[k]

# Left sidebar for chat history
with st.sidebar:
    st.logo("https://learn.microsoft.com/en-us/samples/azure-samples/nlp-sql-in-a-box/nlp-sql-in-a-box/media/banner-nlp-to-sql-in-a-box.png", size="large")

    st.header("🔑 Authentication")
    
    databricks_pat = st.text_input("Databricks Token", key="Databricks PAT", type="password")
    "[Get a Databricks Token](https://docs.databricks.com/aws/en/dev-tools/auth/pat#create-personal-access-tokens-for-workspace-users)"

    if not databricks_pat:
        st.info("Please login to continue.")
        st.stop()

    st.sidebar.divider()

    st.header("💬 Chats")

    # Button to start a new chat
    if st.button("➕ New Chat"):
        st.session_state.conversation_id = None
        st.session_state.messages = []
        st.session_state.selected_chat = None
        st.session_state.new_chat_started = True
        st.session_state.chat_selector = None
        st.session_state.show_examples = True
        st.rerun()

    # Search input
    search_query = st.text_input("Search chats")

# Retrieve previous conversations from Genie
    try: 
        # Call Genie Client with caching
        all_conversations = get_cached_conversations(databricks_pat, GENIE_SPACE)

        # Filter search coincidences on visible chats
        filtered_chats = [
            conv for conv in all_conversations
            if isinstance(conv, dict) and search_query.lower() in conv.get("title", "").lower()
        ] if search_query else all_conversations

        for conv in filtered_chats:
            conv_id = conv.get("conversation_id", "")
            conv_title = conv.get("title", "Untitled Chat")

            with st.container():
                cols = st.columns([0.9, 0.1])

                with cols[0]:
                    # Chat title as button to open conversation
                    if st.button(conv_title, key=f"open_{conv_id}", use_container_width=True):
                        st.session_state.show_examples = False
                        st.session_state.conversation_id = conv_id
                        st.session_state.selected_chat = conv
                        st.info(f"🗂️ **Opened chat:** {conv_title}")

                        # Clean all ratings stored when switching conversations
                        keys_to_delete = [k for k in st.session_state.keys() if k.startswith("rating_")]
                        for k in keys_to_delete:
                            del st.session_state[k]

                        # Load messages for the selected conversation
                        try:
                            messages = get_cached_messages(databricks_pat, GENIE_SPACE, conv_id)
                            sorted_msgs = sorted(messages, key=lambda m: m.get("created_timestamp", 0))
                            chat_history = []
                            for m in sorted_msgs:
                                user_text = m.get("content", "")
                                attachments = m.get("attachments", [])
                                message_id = m.get("message_id")

                                # User message
                                if user_text:
                                    chat_history.append({"role": "user", 
                                                         "content": user_text
                                                         })
                                    
                                # Assistant message
                                if attachments:
                                    query_info = attachments[0].get("query", {})
                                    description = query_info.get("description", "")
                                    sql_query = query_info.get("query", "")

                                    assistant_text = description
                                    if sql_query:
                                        assistant_text += f"\n\n```sql\n{sql_query}\n```"
                                    
                                    # Store message_id and rating on chat history
                                    message_data = {"role": "assistant", 
                                                    "content": assistant_text,
                                                    "message_id": message_id, 
                                                    "query_text": sql_query if sql_query else None
                                                    }
                                    chat_history.append(message_data)
                                    
                                    # If API call returns a rating, store it in session_state
                                    if m.get("rating"):
                                        st.session_state[f"rating_{message_id}"] = m["rating"]
                                    
                            st.session_state.messages = chat_history

                        except Exception as e:
                            st.warning(f"Couldn't load messages for this conversation: {str(e)}")

                with cols[1]:
                    # Popover menu for delete/download actions
                    with st.popover("", use_container_width=True):
                        # Delete conversation
                        if st.button("🗑️ Delete", key=f"delete_{conv_id}"):
                            try:
                                delete_conversation(databricks_pat, GENIE_SPACE, conv_id)
                                st.success(f"Conversation '{conv_title}' deleted successfully.")
                                st.cache_data.clear()
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to delete conversation: {str(e)}")

                        # Download conversation as .txt
                        try:
                            messages = get_cached_messages(databricks_pat, GENIE_SPACE, conv_id)
                            sorted_msgs = sorted(messages, key=lambda m: m.get("created_timestamp", 0))
                            chat_history = []
                            for m in sorted_msgs:
                                user_text = m.get("content", "")
                                attachments = m.get("attachments", [])

                                # User message
                                if user_text:
                                    chat_history.append({"role": "user", 
                                                         "content": user_text
                                                         })
                                    
                                # Assistant message
                                if attachments:
                                    query_info = attachments[0].get("query", {})
                                    description = query_info.get("description", "")
                                    sql_query = query_info.get("query", "")
                                    assistant_text = description

                                    if sql_query:
                                        assistant_text += f"\n\n```sql\n{sql_query}\n```"

                                    chat_history.append({"role": "assistant", 
                                                         "content": assistant_text
                                                         })
                            
                            chat_text = "\n\n".join([f"**{m['role'].capitalize()}**:\n{m['content']}"
                                                    for m in chat_history
                                                    ])
                            st.download_button(
                                    label="📥 Download chat",
                                    data=chat_text,
                                    file_name=f"{conv_title}.txt",
                                    mime="text/plain",
                                    use_container_width=True
                                    )
                        except Exception as e:
                            st.warning(f"Couldn't prepare download: {str(e)}")

    # Message if no chats found
        if not filtered_chats:
            st.info("No previous chats found.")

    except Exception as e:
        st.error(f"⚠️ Couldn't fetch previous chats: {str(e)}")

# Add help button to download 'how to ask' guide
with st.container(horizontal_alignment="right", vertical_alignment="bottom"):
    with open("text-to-sql.md", "r", encoding="utf-8") as f:
        guidance_md = f.read()

    st.download_button(
            label="Download How to Ask Guidance",
            data=guidance_md,
            file_name="text-to-sql.md",
            mime="text/markdown",
            icon=":material/download:",
            type="tertiary",
            help="Download a markdown file with guidance on how to ask questions.",
            on_click="ignore"
            )
    
# Reset messages if no conversation is selected
if "conversation_id" not in st.session_state or st.session_state.conversation_id is None:
    st.session_state.messages = []

# Display chat messages history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if isinstance(message["content"], pd.DataFrame):
            if message.get("text_display"):
                st.markdown(message["text_display"])
            st.dataframe(message["content"])
        else:
            st.markdown(message["content"])

        # Show feedback for assistant messages
        if message["role"] == "assistant" and message.get("message_id"):
            message_id = message["message_id"]
            current_rating = st.session_state.get(f"rating_{message_id}")

            col_up, col_down = st.columns([0.1, 0.9])
        
        # Show buttons if no previous feedback/if feedback is already given
            with col_up:
                if current_rating == "POSITIVE":
                    st.markdown("👍 **(Sent)**") # Marked as positive
                else:
                    st.button("👍", key=f"hist_up_{message_id}", 
                                on_click=send_feedback_callback, 
                                args=(st.session_state.conversation_id, message_id, "POSITIVE"),
                                use_container_width=True, 
                                disabled=bool(current_rating)) # Disabled if already rated
                
            with col_down:
                if current_rating == "NEGATIVE":
                    st.markdown("👎 **(Sent)**") # Marked as negative
                else:
                    st.button("👎", key=f"hist_down_{message_id}", 
                                on_click=send_feedback_callback, 
                                args=(st.session_state.conversation_id, message_id, "NEGATIVE"),
                                use_container_width=False, 
                                disabled=bool(current_rating)) # Disabled if already rated
                
        if message.get("query_text"):
            with st.expander("See generated SQL query"):
                st.code(message["query_text"], language="sql")

# Example prompts
example_prompts = [
    "What is the count of vins by model year?",
    "How many vins are by region?",
    "Return total count of vins"
]

# Render example questions as interactive buttons
if st.session_state.show_examples:
    st.markdown("#### 💡 Sample questions")
    cols = st.columns(len(example_prompts))

    for i, prompt_text in enumerate(example_prompts):
        if cols[i].button(prompt_text, use_container_width=True):
            st.session_state["prefill_prompt"] = prompt_text
            st.session_state["auto_send"] = True
            st.session_state.show_examples = False
            st.rerun()

prefill = st.session_state.get("prefill_prompt", "")
placeholder_text = prefill if prefill else "Ask your question..."

# Accept User input
prompt = st.chat_input(placeholder=placeholder_text, key="user_input", accept_file=True, file_type=["csv", "xlsx"])

if prompt:
    user_text = prompt.text if hasattr(prompt, "text") else str(prompt)
    uploaded_files = getattr(prompt, "files", None)
    st.session_state.show_examples = False
    st.session_state["prefill_prompt"] = ""
elif st.session_state.get("auto_send"):
    user_text = st.session_state["prefill_prompt"]
    uploaded_files = None
    st.session_state["auto_send"] = False
    st.session_state["prefill_prompt"] = ""
else:
    user_text = None
    uploaded_files = None

# Process user input
if user_text:
    st.session_state.messages.append({"role": "user", "content": user_text})
    st.session_state.show_examples = False
    with st.chat_message("user"):
        st.markdown(user_text)
        if uploaded_files:
            for file in uploaded_files:
                st.caption(f"📎 Attached file: {file.name}")

    try:
        with st.spinner("Querying Genie..."):
            if uploaded_files:
                file = uploaded_files[0]
                attachment_bytes = file.read()
                filename = file.name
            else:
                attachment_bytes = None
                filename = None

            # Initialize assistant message ID
            assistant_message_id = ""

            # Start new conversation if there is no history
            if st.session_state.conversation_id is None:
                conv_id, result, query_text, assistant_message_id = start_new_conversation(
                    user_text, 
                    databricks_pat, 
                    GENIE_SPACE,
                    attachment=attachment_bytes,
                    filename=filename
                )
                st.session_state.conversation_id = conv_id
                st.cache_data.clear()
            else:
                # Continue existing conversation
                result, query_text, assistant_message_id = continue_conversation(
                    st.session_state.conversation_id, 
                    user_text, 
                    databricks_pat, 
                    GENIE_SPACE,
                    attachment=attachment_bytes,
                    filename=filename
                )
                st.cache_data.clear()

            # Store last assistant message ID for feedback
            st.session_state["last_message_id"] = assistant_message_id

        # Process assistant result
        # If string, process as text response
        if isinstance(result, str):
            message_data = {"role": "assistant", "content": result, "message_id": assistant_message_id}
            if query_text:
                message_data["query_text"] = query_text
            st.session_state.messages.append(message_data)

            # Render and show feedback buttons
            with st.chat_message("assistant"):
                st.markdown(result)
                
                col_up, col_down = st.columns([0.1, 0.9])

                with col_up:
                    st.button("👍", key=f"up_{assistant_message_id}", 
                                    on_click=send_feedback_callback, 
                                    args=(st.session_state.conversation_id, assistant_message_id, "POSITIVE"),
                                    use_container_width=True)

                with col_down:
                    st.button("👎", key=f"down_{assistant_message_id}", 
                                    on_click=send_feedback_callback, 
                                    args=(st.session_state.conversation_id, assistant_message_id, "NEGATIVE"),
                                    use_container_width=False)
                    
                if query_text:
                    with st.expander("See generated SQL query"):
                        st.code(query_text, language="sql")

        # If DF, process as dataframe response
        elif isinstance(result, pd.DataFrame):
            message_data = {"role": "assistant", "content": result, "message_id": assistant_message_id}
            if query_text:
                message_data["query_text"] = query_text
            st.session_state.messages.append(message_data)

            # Render and show feedback buttons
            with st.chat_message("assistant"):
                st.dataframe(result)
                
                col_up, col_down = st.columns([0.1, 0.9])

                with col_up:
                    st.button("👍", key=f"up_{assistant_message_id}", 
                                    on_click=send_feedback_callback, 
                                    args=(st.session_state.conversation_id, assistant_message_id, "POSITIVE"),
                                    use_container_width=True)

                with col_down:
                    st.button("👎", key=f"down_{assistant_message_id}", 
                                    on_click=send_feedback_callback, 
                                    args=(st.session_state.conversation_id, assistant_message_id, "NEGATIVE"),
                                    use_container_width=False)

            if query_text:
                with st.expander("See generated SQL query"):
                    st.code(query_text, language="sql")

        else:
            st.warning("Genie didn't return results.")
    
    # Handle errors
    except Exception as e:
        logger.error(f"Error while querying Genie: {str(e)}")
        st.error(f"❌ An error arised: {str(e)}")