from multiprocessing import context
import streamlit as st
from genie_room import start_new_conversation, continue_conversation, delete_conversation, execute_sql_with_polling, semantic_search
from databricks.sdk.service.dashboards import GenieFeedbackRating
from dotenv import load_dotenv
import logging
import os
import pandas as pd

# Load environment variables
load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
HTTP_PATH = os.environ.get("HTTP_PATH")
CATALOG = os.environ.get("CATALOG")
SCHEMA = os.environ.get("SCHEMA")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# User information management
# Grab and store Databricks current user_id
def current_user():
    from genie_room import current_user 
    pat = st.session_state.get("Databricks PAT")
    space_id = st.session_state.get("GENIE_SPACE")
    if not pat or not space_id:
        pass
    try:
        current_user_information = current_user(space_id, pat)
        return current_user_information
    
    except Exception as e:
        logger.error(f"Couldn't grab current user id: {str(e)}")
        return None

# Insert/Update user_info Database
def user_info(user: dict):
    from databricks import sql
    pat = st.session_state.get("Databricks PAT")
    space_id = st.session_state.get("GENIE_SPACE")
    if not pat or not space_id:
        pass
    try:
        user_groups_list = "ARRAY(" + ", ".join(f"'{x}'" for x in user["groups"]) + ")"
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=HTTP_PATH,
            access_token=pat
        ) as conn:
            cursor = conn.cursor()
            # Check existence
            cursor.execute(f"""
                            SELECT 1
                            FROM {CATALOG}.{SCHEMA}.users_info
                            WHERE user_id = ?
                            LIMIT 1
                            """, (st.session_state["current_user_id"],))

            exists = cursor.fetchone()

            # Insert if not exists, update if exists
            if not exists:
                cursor.execute(f"""
                    INSERT INTO {CATALOG}.{SCHEMA}.users_info
                    (user_id, user_name, email, groups, first_login_timestamp, last_login_timestamp, total_logins)
                    VALUES (?, ?, ?, {user_groups_list}, current_timestamp(), current_timestamp(), 1)
                """, (user["user_id"], user["user_name"],user["email"],))

                logger.info(f"Inserted new user {user['user_id']} into users_info")

            else:
                cursor.execute(f"""
                                UPDATE {CATALOG}.{SCHEMA}.users_info
                                SET last_login_timestamp = current_timestamp(), total_logins = total_logins + 1
                                WHERE user_id = ?
                                """, (user["user_id"],))
                
                logger.info(f"Updated existing user {user['user_id']} in users_info")

    except Exception as e:
        logger.error(f"Error ensuring user exists: {str(e)}")

    finally:
        try:
            cursor.close()
            conn.close()
            logging.info("Closed Databricks SQL connection successfully.")
        except Exception as close_err:
            logging.warning(f"Error closing connection: {str(close_err)}")

# Data retrieval
# Bulk load conversations and messages
def initial_load():
    from databricks import sql
    pat = st.session_state.get("Databricks PAT")
    space_id = st.session_state.get("GENIE_SPACE")
    user_id = st.session_state.get("current_user_id")
    if not pat or not space_id:
        pass
    try:
        with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=HTTP_PATH,
            access_token=pat
        ) as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                            SELECT conversation_id, COALESCE(ai_title, chat_title) AS title, created_timestamp
                            FROM {CATALOG}.{SCHEMA}.conversations
                            WHERE space_id = ? AND user_id = ?
                            ORDER BY created_timestamp DESC
                            """, (space_id, user_id,))
            convs_rows = cursor.fetchall()
            convs_cols = [c[0] for c in cursor.description]
            all_conversations = [dict(zip(convs_cols, r)) for r in convs_rows]

            cursor.execute(f"""
                            SELECT *
                            FROM {CATALOG}.{SCHEMA}.messages
                            WHERE user_id = ?
                            ORDER BY created_timestamp ASC
                            """, (user_id,))
            msgs_rows = cursor.fetchall()
            msgs_cols = [c[0] for c in cursor.description]
            all_msgs = [dict(zip(msgs_cols, r)) for r in msgs_rows]

            logging.info(f"Initial load completed: {len(all_conversations)} chats and {len(all_msgs)} messages.")
            return all_conversations, all_msgs
        
    except Exception as e:
        logger.error(f"Couldn't load previous chats and messages: {str(e)}")
        return [], []
    
    finally:
        try:
            cursor.close()
            conn.close()
            logging.info("Closed Databricks SQL connection successfully.")
        except Exception as close_err:
            logging.warning(f"Error closing connection: {str(close_err)}")

def transform_db_to_chat(messages):
    """Convierte filas de la DB al formato de mensajes de la App."""
    chat_history = []
    processed_prompts = set()

    for m in messages:
        user_prompt = m.get("prompt", "")
        assistant_response = m.get("completion", "")
        sql_query = m.get("assistant_attachment", "")
        m_id = m.get("message_id")
        regen_df = m.get("regenerated_df")

        # User message
        if user_prompt and m_id not in processed_prompts:
            chat_history.append({"role": "user", "content": user_prompt})
            processed_prompts.add(m_id)
        
        # Assistant message
        if assistant_response or regen_df is not None:
            full_content = regen_df if regen_df is not None else assistant_response
            if sql_query and regen_df is None: # If not regenerated, add the SQL
                 if f"```sql\n{sql_query}\n```" not in str(full_content):
                    full_content = f"{full_content}\n\n```sql\n{sql_query}\n```"

            chat_history.append({
                "role": "assistant",
                "content": full_content,
                "message_id": m_id,
                "query_text": sql_query,
                "text_display": assistant_response if regen_df is not None else None
            })
            
            # Sync rating in session_state
            if m.get("rating"):
                st.session_state[f"rating_{m_id}"] = m["rating"]
                
    return chat_history

# Callback functions
# Callback function to regenerate SQL result
def regenerate_sql_callback(message_id, sql_text, context):
    """Re-executes SQL Statement and replace message content in session_state."""
    pat = st.session_state.get("Databricks PAT")
    space_id = st.session_state.get("GENIE_SPACE")
    use_external = st.session_state.get("use_external_results", False)
    try:
        df = execute_sql_with_polling(
            space_id=space_id,
            token=pat,
            http_path=HTTP_PATH,
            catalog=CATALOG,
            schema=SCHEMA,
            warehouse_id=WAREHOUSE_ID,
            message_id=message_id,
            sql_text=sql_text,
            use_external=use_external
        )

        # Chat context
        # Update message within session_state
        if context == "chat":
            if "regenerated_results" not in st.session_state:
                st.session_state.regenerated_results = {}
            st.session_state.regenerated_results[message_id] = df

            if "all_user_messages" in st.session_state:
                for msg in st.session_state.all_user_messages:
                    if msg.get("message_id") == message_id:
                        msg["content"] = df
                        base_text = msg.get("text_display", "")
                        regen_text = f"\n\n🔄 Result regenerated at {pd.Timestamp.utcnow()}"
                        msg["text_display"] = base_text + regen_text
                        msg["regenerated_df"] = df
                        break

            if "messages" in st.session_state:
                for m in st.session_state.messages:
                    if m.get("message_id") == message_id:
                        m["content"] = df
                        m["text_display"] = f"🔄 Result regenerated at {pd.Timestamp.utcnow().strftime('%H:%M:%S')}"
                        break

        # Semantic search context
        if context == "semantic":
            if "semantic_regenerated_results" not in st.session_state:
                st.session_state.semantic_regenerated_results = {}

            st.session_state.semantic_regenerated_results[message_id] = {
                "df": df,
                "timestamp": pd.Timestamp.utcnow()
            }

            if "semantic_expanded" not in st.session_state:
                st.session_state.semantic_expanded = {}
                st.session_state.semantic_expanded[message_id] = True

    except Exception as e:
        logger.error(f"SQL Regeneration failed: {str(e)}")
        st.error(f"Error regenerating SQL result: {e}")

# Callback function to run semantic search
def run_semantic_search(semantic_query):
    pat = st.session_state.get("Databricks PAT")
    space_id = st.session_state.get("GENIE_SPACE")
    sem_results = semantic_search(space_id, pat, HTTP_PATH, CATALOG, SCHEMA, semantic_query)
    normalized = []
    for r in sem_results:
        normalized.append({
                    "prompt": r[0],
                    "completion": r[1],
                    "assistant_attachment": r[2],
                    "message_id": r[3],
                    "user_id": r[4],
                    "score": r[-1]
                    })
    normalized.sort(key=lambda x: x["score"], reverse=True)
    st.session_state.semantic_results = normalized
    st.session_state.semantic_user_ids = list({r["user_id"] for r in normalized})

# Load users info for semantic search results
def load_users_info(user_ids):
    from databricks import sql
    placeholders = ",".join(["?"] * len(user_ids))

    with sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=HTTP_PATH,
            access_token=st.session_state.get("Databricks PAT")
        ) as conn:
        cursor = conn.cursor()
        cursor.execute(f"""
                        SELECT user_id, user_name, email
                        FROM {CATALOG}.{SCHEMA}.users_info
                        WHERE user_id IN ({placeholders})
                        """, tuple(user_ids))
        rows = cursor.fetchall()

    return {r[0]: {"user_name": r[1], "user_email": r[2]} for r in rows}

def reset_search():
    st.session_state.semantic_results = None
    st.session_state.semantic_expanded = {}
    st.session_state["semantic_query_prompt"] = ""

@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

# Callback function to send feedback
def send_feedback_callback(conversation_id: str, message_id: str, rating_str):
    """Sends feedback and registers rating in session_state."""
    from genie_room import send_message_feedback
    pat = st.session_state.get("Databricks PAT")
    space_id = st.session_state.get("GENIE_SPACE")
    if not pat or not space_id:
        pass
    try:
        send_message_feedback(
            pat,
            space_id,
            conversation_id=conversation_id,
            message_id=message_id, 
            rating=rating_str,
            http_path=HTTP_PATH,
            catalog=CATALOG,
            schema=SCHEMA
        )

        # Update all_user_messages in session_state
        if "all_user_messages" in st.session_state:
            for msg in st.session_state.all_user_messages:
                if msg.get("message_id") == f"assistant_{message_id}":
                    msg["rating"] = str(rating_str).split('.')[-1] 
                    break

        # Register feedback for visual persistence
        st.session_state[f"rating_{message_id}"] = rating_str.value # Stores 'positive' or 'negative'
        st.toast(f"✅ Feedback '{str(rating_str)}' sent. Thanks!", icon="💬")  
    except Exception as e:
        logger.error(f"Feedback submission failed: {str(e)}")
        st.toast("⚠️ Error while sending feedback", icon="⚠️")

# Page configuration
st.set_page_config(
    page_title="<team_name> Bot powered by Genie", #TabularAI
    page_icon=":streamlit:",
    layout=None,
    initial_sidebar_state="expanded",
    menu_items={
        'About': "# This is a <team_name> product."
    }
)

# App title
st.title("Genie Bot 🤖") #TabularAI

# Initialize chat history (If no messages)
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None
if "messages" not in st.session_state:
    st.session_state.messages = []
if "show_examples" not in st.session_state:
    st.session_state.show_examples = True  # Show examples
if "active_tab" not in st.session_state:
    st.session_state.active_tab = "chat"

# Reset conversation if new chat started
if "new_chat_started" in st.session_state and st.session_state.new_chat_started:
    st.session_state.new_chat_started = False
    st.session_state.conversation_id = None
    st.session_state.messages = []
    st.session_state.show_examples = True # Show examples in new chat

    # Clean all ratings stored when new chat starts
    #keys_to_delete = [k for k in st.session_state.keys() if k.startswith("rating_")]
    #for k in keys_to_delete:
    #    del st.session_state[k]

# Left sidebar for chat history
with st.sidebar:
    st.logo("https://learn.microsoft.com/en-us/samples/azure-samples/nlp-sql-in-a-box/nlp-sql-in-a-box/media/banner-nlp-to-sql-in-a-box.png", size="large")

    st.header("🔑 Authentication")
    
    databricks_pat = st.text_input("Databricks Token", key="Databricks PAT", type="password")
    "[Get a Databricks Token](https://docs.databricks.com/aws/en/dev-tools/auth/pat#create-personal-access-tokens-for-workspace-users)"

    genie_id = st.text_input("Genie ID", key="GENIE_SPACE", type="password")

    if not databricks_pat or not genie_id:
        st.info("Please login to continue.")
        st.stop()

    # Store current_user_id in session_state
    if "current_user_id" not in st.session_state:
        current_user_info = current_user()
        if current_user_info:
            st.session_state.current_user_id = current_user_info["user_id"]
            if "user_tracked" not in st.session_state:
                user_info(current_user_info)
                st.session_state["user_tracked"] = True

    st.sidebar.divider()

    st.header("🖱️ Navigation")
    choice = st.sidebar.radio("Navigation", ["💬 Chat", "🔍 Semantic Search"], horizontal=True, label_visibility="collapsed")
    st.session_state.active_tab = "semantic_search" if choice == "🔍 Semantic Search" else "chat"

    st.sidebar.divider()

    st.header("⚙️ Settings")
    use_external = st.checkbox(
        "External results (large datasets)",
        value=False,
        help="Enable this option to fetch large result sets (30k + rows)."
    )

    st.session_state.use_external_results = use_external
    if st.session_state.get("use_external_results"):
        st.info("External results enabled. Large datasets may take longer to load.")

    st.sidebar.divider()

    if st.session_state.active_tab == "semantic_search":
        st.markdown("🔍 **Semantic search active**")
        st.info("Switch back to **Chat** tab to continue conversations.")
    else:
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
            # Call Databricks backend database
            if "all_user_messages" not in st.session_state:
                with st.spinner("Querying database..."):
                    conversations, messages = initial_load()
                    st.session_state.all_conversations = conversations
                    st.session_state.all_user_messages = messages
    
            # Filter search coincidences on visible chats
            filtered_chats = [
                conv for conv in st.session_state.all_conversations
                if isinstance(conv, dict) and search_query.lower() in conv.get("title", "").lower()
            ] if search_query else st.session_state.all_conversations
    
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
                            #keys_to_delete = [k for k in st.session_state.keys() if k.startswith("rating_")]
                            #for k in keys_to_delete:
                            #    del st.session_state[k]
    
                            # Load messages for the selected conversation
                            try:
                                messages = [m for m in st.session_state.all_user_messages if m["conversation_id"] == conv_id]      
                                st.session_state.messages = transform_db_to_chat(messages)
                                st.rerun()
    
                            except Exception as e:
                                st.warning(f"Couldn't load messages for this conversation: {str(e)}")
    
                    with cols[1]:
                        # Popover menu for delete/download actions
                        with st.popover("", use_container_width=True):
                            # Delete conversation
                            if st.button("🗑️ Delete", key=f"delete_{conv_id}"):
                                try:
                                    delete_conversation(databricks_pat, genie_id, conv_id, HTTP_PATH, CATALOG, SCHEMA)

                                    # Clean conversation from session_state
                                    if "all_conversations" in st.session_state:
                                        st.session_state.all_conversations = [c for c in st.session_state.all_conversations 
                                                                              if c.get("conversation_id") != conv_id]

                                    # Clean conversation messages from session_state                                       
                                    if "all_user_messages" in st.session_state:
                                        st.session_state.all_user_messages = [m for m in st.session_state.all_user_messages 
                                                                              if m.get("conversation_id") != conv_id]

                                    # Reset current conversation if it was the deleted one
                                    if st.session_state.get("conversation_id") == conv_id:
                                        st.session_state.conversation_id = None
                                        st.session_state.messages = []
                                        st.session_state.last_message_id = None

                                    st.success(f"Conversation '{conv_title}' deleted successfully.")
                                    st.session_state.show_examples = True
                                    st.rerun()

                                except Exception as e:
                                    st.error(f"Failed to delete conversation: {str(e)}")
    
                            # Download conversation as .txt
                            try:
                                raw_messages = [m for m in st.session_state.all_user_messages if m["conversation_id"] == conv_id]
                                formatted_dl = transform_db_to_chat(raw_messages)
                                
                                chat_text = "\n\n".join([f"**{m['role'].capitalize()}**:\n{m['content']}"
                                                        for m in formatted_dl
                                                        ])
                                st.download_button(
                                        label="📥 Download chat",
                                        data=chat_text,
                                        file_name=f"{conv_title}.txt",
                                        mime="text/plain",
                                        key=f"dl_{conv_id}",
                                        use_container_width=True
                                        )
                            except Exception as e:
                                st.warning(f"Couldn't prepare download: {str(e)}")
    
            # Message if no chats found
            if not filtered_chats:
                st.info("No previous chats found.")
    
        except Exception as e:
            st.error(f"⚠️ Couldn't fetch previous chats: {str(e)}")

if st.session_state.active_tab == "chat":
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
    rendered_user_prompts = set()
    for message in st.session_state.get("messages", []):
        role = message.get("role")
        content = message.get("content")
        message_id = message.get("message_id")
        query_text = message.get("query_text")

        # Verify if user message has been rendered before to avoid duplicates
        if role == "user":
            prompt_hash = f"{message_id}_{content}"
            if prompt_hash in rendered_user_prompts:
                continue
            rendered_user_prompts.add(prompt_hash)
    
        # Use current_message to render
        with st.chat_message(role):
            if isinstance(content, pd.DataFrame):
                if message.get("text_display"):
                    st.markdown(message.get("text_display"))
                st.dataframe(content)
            else:
                st.markdown(content)

            # Show feedback for assistant messages
            if role == "assistant" and message_id and query_text:
                col_reg, col_dl = st.columns([0.3, 0.7])

                with col_reg:
                    if st.button(
                        "🔄 Regenerate result",
                        key=f"regen_{message_id}",
                        help="Re-run this SQL query to refresh the result"
                    ):
                        regenerate_sql_callback(
                            message_id=message_id,
                            sql_text=query_text,
                            context="chat"
                        )
                        st.rerun()

                # Download only if DataFrame available
                if isinstance(content, pd.DataFrame):
                    with col_dl:
                        csv_data = convert_df(content)
                        st.download_button(
                            label="📥 Download Full Data (CSV)",
                            data=csv_data,
                            file_name=f"full_results_{message_id}.csv",
                            mime="text/csv",
                            key=f"btn_dl_{message_id}",
                            use_container_width=False
                        )

                current_rating = st.session_state.get(f"rating_{message_id}")

                col_up, col_down = st.columns([0.1, 0.9])

                # Show buttons if no previous feedback/if feedback is already given
                with col_up:
                    if current_rating == "POSITIVE":
                        st.markdown("👍 **(Sent)**")
                    else:
                        st.button("👍", key=f"hist_up_{message_id}", 
                             on_click=send_feedback_callback, 
                             args=(st.session_state.conversation_id, message_id, GenieFeedbackRating.POSITIVE),
                             use_container_width=True, 
                             disabled=bool(current_rating)) # Disabled if already rated

                with col_down:
                    if current_rating == "NEGATIVE":
                        st.markdown("👎 **(Sent)**")
                    else:
                        st.button("👎", key=f"hist_down_{message_id}", 
                             on_click=send_feedback_callback, 
                             args=(st.session_state.conversation_id, message_id, GenieFeedbackRating.NEGATIVE),
                             use_container_width=False, 
                             disabled=bool(current_rating))

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

    # Spacer to keep input at bottom
    st.markdown(
    """
    <div style="flex-grow: 1; height: 22vh;"></div>
    """,
    unsafe_allow_html=True
    )

    # Accept User input
    prompt = st.chat_input(placeholder=placeholder_text, key="user_input") # accept_file=True, file_type=["csv", "xlsx"]

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
        st.session_state.show_examples = False
    else:
        user_text = None
        uploaded_files = None

    # Process user input
    if user_text:
        st.session_state.messages.append({"role": "user", "content": user_text})
        st.session_state.show_examples = False

        if "all_user_messages" in st.session_state:
            st.session_state.all_user_messages.append({
                                "conversation_id": st.session_state.conversation_id,
                                "prompt": user_text,
                                "role": "user",
                                "user_id": st.session_state.get("current_user_id"),
                                "created_timestamp": pd.Timestamp.utcnow()})

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
                    conv_id, result, query_text, assistant_message_id, assistant_description, ai_title = start_new_conversation(
                        user_text, 
                        databricks_pat, 
                        genie_id,
                        HTTP_PATH,
                        CATALOG,
                        SCHEMA,
                        attachment=attachment_bytes,
                        filename=filename
                    )

                    st.session_state.conversation_id = conv_id
                    # Update conversations list in session_state
                    new_chats = {"conversation_id": conv_id,
                                 "title": ai_title,
                                 "created_timestamp": pd.Timestamp.utcnow()}
                    
                    if "all_conversations" in st.session_state:
                        st.session_state.all_conversations.insert(0, new_chats)
                    else:
                        st.session_state.all_conversations = [new_chats]

                    #st.rerun()
                    
                else:
                    # Continue existing conversation
                    result, query_text, assistant_message_id, assistant_description = continue_conversation(
                        st.session_state.conversation_id, 
                        user_text, 
                        databricks_pat, 
                        genie_id,
                        HTTP_PATH,
                        CATALOG,
                        SCHEMA,
                        attachment=attachment_bytes,
                        filename=filename
                    )

                # Store last assistant message ID for feedback
                st.session_state["last_message_id"] = assistant_message_id

            # Update all user messages with user message
            if "all_user_messages" in st.session_state:
                st.session_state.all_user_messages.append({
                                    "conversation_id": st.session_state.conversation_id,
                                    "message_id": f"user_{assistant_message_id}",
                                    "prompt": user_text,
                                    "role": "user",
                                    "user_id": st.session_state.get("current_user_id"),
                                    "created_timestamp": pd.Timestamp.utcnow()})
                
            # Process assistant result
            # If string, process as text response
            if isinstance(result, str):
                message_data = {"role": "assistant", "content": result, "message_id": assistant_message_id}
                if query_text:
                    message_data["query_text"] = query_text
                st.session_state.messages.append(message_data)

                # Update all user messages
                if "all_user_messages" in st.session_state:
                    st.session_state.all_user_messages.append({
                                    "conversation_id": st.session_state.conversation_id,
                                    "message_id": f"assistant_{assistant_message_id}",
                                    "prompt": user_text,
                                    "completion": result,
                                    "assistant_attachment": query_text,
                                    "role": "assistant",
                                    "user_id": st.session_state.get("current_user_id"),
                                    "rating": None,
                                    "created_timestamp": pd.Timestamp.utcnow(),
                                    "regenerated_df": None})
                st.rerun()

            # If DF, process as dataframe response
            elif isinstance(result, pd.DataFrame):
                message_data = {"role": "assistant", "content": result, "message_id": assistant_message_id}
                if assistant_description:
                    message_data["text_display"] = assistant_description
                if query_text:
                    message_data["query_text"] = query_text
                st.session_state.messages.append(message_data)

                # Update all user messages
                if "all_user_messages" in st.session_state:
                    st.session_state.all_user_messages.append({
                                    "conversation_id": st.session_state.conversation_id,
                                    "message_id": f"assistant_{assistant_message_id}",
                                    "prompt": user_text,
                                    "completion": assistant_description, # The text alongside DF
                                    "assistant_attachment": query_text,
                                    "role": "assistant",
                                    "user_id": st.session_state.get("current_user_id"),
                                    "rating": None,
                                    "created_timestamp": pd.Timestamp.utcnow(),
                                    "regenerated_df": result})
                st.rerun()

            else:
                st.warning("Genie didn't return results.")

        # Handle errors
        except Exception as e:
            logger.error(f"Error while querying Genie: {str(e)}")
            st.error(f"❌ An error arised: {str(e)}")

else:
    # Semantic search
    if "semantic_results" not in st.session_state:
        st.session_state.semantic_results = None
    
    st.subheader("🔎 Semantic Search")
    semantic_query = st.text_input("Prompt your question", key="semantic_query_prompt")
    col1, col2 = st.columns([1,1])

    col1, col2, _ = st.columns([1,1,6])
    col1.button("🔍 Search", on_click=run_semantic_search, args=(semantic_query,))
    col2.button("♻️ Reset", on_click=reset_search)

    # Show similar results
    if st.session_state.semantic_results:
        
        st.markdown("### 🔗 Matching results")

        user_ids = st.session_state.get("semantic_user_ids", [])
        users_map = load_users_info(user_ids) if user_ids else {}

        for idx, r in enumerate(st.session_state.semantic_results):
            user = users_map.get(r["user_id"], {})
            user_name = user.get("user_name", "Unknown user")
            user_email = user.get("user_email", "")

            st.markdown(f"#### {idx + 1}. {r['prompt']}")
            
            st.caption(f"👤 Written by {user_name} · 📧 Email for contact: {user_email}")

            st.markdown(f"Similarity score: **{r['score']:.2f}**")

            # SQL statement + regeneration feature
            is_open = st.session_state.get("semantic_expanded", {}).get(r["message_id"], False)
            with st.expander("View SQL query", expanded=is_open):
                st.code(r["assistant_attachment"], language="sql")

                st.button(
                    "🔄 Regenerate SQL result",
                    key=f"sem_regen_{r['message_id']}",
                    on_click=regenerate_sql_callback,
                    kwargs={
                        "message_id": r["message_id"],
                        "sql_text": r["assistant_attachment"],
                        "context": "semantic"
                    }
                )
                
                regen = st.session_state.get("semantic_regenerated_results", {}).get(r["message_id"])

                if regen:
                    st.caption(f"🔄 Result regenerated at {regen['timestamp']}")
                    st.dataframe(regen["df"])

        st.divider()