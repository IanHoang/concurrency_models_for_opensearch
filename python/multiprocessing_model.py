import time
import multiprocessing
import os

from opensearchpy import OpenSearch
from dotenv import load_dotenv

from opensearch_client import create_client, extract_documents

def run_multiprocessing(indices, client):
    start_time = time.time()

    processes = []
    for index in indices:
        process = multiprocessing.Process(target=extract_documents, args=(index,client))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    end_time = time.time()
    print(f"Multiprocessing: {end_time - start_time} seconds")


if __name__ == "__main__":
    load_dotenv()
    host = os.getenv('TARGET_HOST')
    port = 443
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    user_auth = (username, password)

    client = create_client(host, port, user_auth)
    indices = ['A', 'B', 'C']

    run_multiprocessing(indices, client)