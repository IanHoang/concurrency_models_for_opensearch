import os
import time
import threading

from opensearchpy import OpenSearch
from dotenv import load_dotenv

from opensearch_client import create_client, extract_documents

def run_threading(indices, client):
    start_time = time.time()

    threads = []
    for index in indices:
        thread = threading.Thread(target=extract_documents, args=(index,client,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time.time()
    print(f"Threading: {end_time - start_time} seconds")


if __name__ == "__main__":
    load_dotenv()
    host = os.getenv('TARGET_HOST')
    port = 443
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    user_auth = (username, password)

    client = create_client(host, port, user_auth)
    indices = ['A', 'B', 'C']

    run_threading(indices, client)


    