import os

import opensearchpy
from requests_aws4auth import AWS4Auth
from dotenv import load_dotenv


# Extract standard opensearch-py approach
def extract_documents(index_name, client):
    # Perform a match_all query and scroll through documents in the index
    query = {"query": {"match_all": {}}}
    response = client.search(index=index_name, body=query, scroll='2m', size=1000)

    scroll_id = response['_scroll_id']
    total_docs = len(response['hits']['hits'])

    while len(response['hits']['hits']):
        response = client.scroll(scroll_id=scroll_id, scroll='2m')
        total_docs += len(response['hits']['hits'])

    return total_docs

def create_client(host, port, user_auth):
    # Works for both managed FGAC service domains and self-managed domains
    return opensearchpy.OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_auth = user_auth,
        use_ssl = True,
        ssl_show_warn = False,
        verify_certs = False,
        connection_class = opensearchpy.Urllib3HttpConnection
    )


if __name__ == "__main__":
    load_dotenv()
    host = os.getenv('TARGET_HOST')
    port = 443
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    user_auth = (username, password)

    client = create_client(host, port, user_auth)

    # Test cat indices
    print(client.cat.indices())



