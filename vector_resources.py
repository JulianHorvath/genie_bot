from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType, DeltaSyncVectorIndexSpecRequest, EmbeddingSourceColumn, PipelineType, VectorIndexType
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABRICKS_HOST= os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN= os.environ.get("DATABRICKS_TOKEN")
CATALOG = os.environ.get("CATALOG")
SCHEMA = os.environ.get("SCHEMA")


#####################################
### Create vector search endpoint ###
#####################################

def create_vector_search_endpoint(endpoint_name: str):
    """Creates a Vector Search endpoint if it does not already exist."""
    w = WorkspaceClient(host=f"https://{DATABRICKS_HOST}", token=DATABRICKS_TOKEN)

    # Check if endpoint already exists
    try:
        existing = w.vector_search_endpoints.get_endpoint(endpoint_name=endpoint_name)
        print(f"Vector Search endpoint '{endpoint_name}' already exists.")
        return existing
    except Exception:
        pass

    # Create endpoint
    print(f"Creating Vector Search endpoint '{endpoint_name}'...")
    endpoint = w.vector_search_endpoints.create_endpoint(
        name=endpoint_name,
        endpoint_type=EndpointType.STANDARD #STANDARD
    )

    # Wait for endpoint to become ready
    w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(endpoint_name=endpoint_name)
    print(f"Endpoint '{endpoint_name}' is ready.")

    return endpoint

# Define variables and execute function
endpoint_name = "vsc_stchat_endpoint"
create_vector_search_endpoint(endpoint_name=endpoint_name)

##################################
### Create vector search index ###
##################################

def create_vector_search_index(endpoint_name: str, source_table: str, index_name: str, index_type: VectorIndexType, pipeline_type: PipelineType, primary_key: str, embedding_source_column: str, embedding_model_endpoint_name: str):
    """
    Creates a Vector Search index if it does not already exist.
    """
    w = WorkspaceClient(host=f"https://{DATABRICKS_HOST}", token=DATABRICKS_TOKEN)

    # Check if index already exists
    try:
        existing = w.vector_search_indexes.get_index(index_name=index_name)
        print(f"Vector Search index '{index_name}' already exists.")
        return existing
    except Exception:
        pass

    embedding_source_columns = [EmbeddingSourceColumn(embedding_model_endpoint_name=embedding_model_endpoint_name, 
                                                                              name=embedding_source_column)]

    spec = DeltaSyncVectorIndexSpecRequest(
        embedding_source_columns=embedding_source_columns,
        pipeline_type=pipeline_type,
        source_table=source_table
    )

    # Create index
    print(f"Creating Vector Search index '{index_name}'...")
    index = w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=endpoint_name,
        primary_key=primary_key,
        index_type=index_type,
        delta_sync_index_spec=spec
    )

    print(f"Index '{index_name}' created.")

    return index

# Define variables and execute function
#endpoint_name = "vsc_stchat_endpoint"
source_table = f"{CATALOG}.{SCHEMA}.messages"
index_name = f"{CATALOG}.{SCHEMA}.messages_user_questions_index"
index_type = VectorIndexType.DELTA_SYNC
pipeline_type = PipelineType.TRIGGERED
primary_key = "message_id"
embedding_model_endpoint_name = "databricks-bge-large-en"
embedding_source_column = "prompt"

create_vector_search_index(endpoint_name, source_table, index_name, index_type, pipeline_type, primary_key, embedding_source_column, embedding_model_endpoint_name)