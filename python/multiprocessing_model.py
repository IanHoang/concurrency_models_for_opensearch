import time
import multiprocessing
import os
import json

from opensearchpy import OpenSearch
from dotenv import load_dotenv

from opensearch_client import create_client, write_documents_to_file

def run_multiprocessing(indices):
    start_time = time.time()

    processes = []
    for index in indices:
        process = multiprocessing.Process(target=extract_documents_for_each_process, args=(index,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    end_time = time.time()
    print(f"Multiprocessing: {end_time - start_time} seconds")

def extract_documents_for_each_process(index_name):
    load_dotenv()
    host = os.getenv('TARGET_HOST')
    port = 443
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    user_auth = (username, password)

    # Need to create a separate client for each process (this is why we have duplicate code introduced in this file)
    client = create_client(host, port, user_auth)

    # Perform a match_all query and scroll through documents in the index
    query = {"query": {"match_all": {}}}
    response = client.search(index=index_name, body=query, scroll='2m', size=1000)

    scroll_id = response['_scroll_id']
    total_docs = len(response['hits']['hits'])

    write_documents_to_file(index_name, response['hits']['hits'])

    while len(response['hits']['hits']):
        response = client.scroll(scroll_id=scroll_id, scroll='2m')
        total_docs += len(response['hits']['hits'])
        write_documents_to_file(index_name, response['hits']['hits'])

    print(f"Extracted {total_docs} documents from {index_name}")

if __name__ == "__main__":
    indices = ['movies-1000', 'movies-2000', 'weather-data-2016']

    run_multiprocessing(indices)