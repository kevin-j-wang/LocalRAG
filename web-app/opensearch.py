import json
from pprint import pprint
import os
import time

from dotenv import load_dotenv
from opensearchpy import OpenSearch
load_dotenv()
PORT = os.getenv("PORT")
API_KEY = os.getenv("API_KEY")
index_name = os.getenv("INDEX_NAME")
auth = ('admin', 'ePiCPasSW0rd1!!!')
#remove overwrite test name later
index_name = "semantic_test"



class Search:
    def __init__(self):
        self.client = OpenSearch(
            hosts = [{'host': 'localhost', 'port': PORT}],
            http_auth = auth,
            use_ssl = True,
            verify_certs = False,
            ssl_show_warn = False,
        )
        client_info = self.client.info()
        print('Connected to OpenSearch!')
        pprint(client_info)

    def create_index(self):
        self.client.indices.delete(index=index_name, ignore_unavailable=True)
        self.client.indices.create(index=index_name)

    def reindex(self):
        self.create_index()
        with open('data.json', 'rt') as f:
            documents = json.loads(f.read())
        return self.insert_documents(documents)
    
    def search(self, **query_args):
        return self.client.search(index=index_name, **query_args)

    '''
    Insert several documents
    '''
    def insert_document(self, document):
        return self.client.index(index=index_name, body=document)

    '''
    Bulk insert documents with 1 api call
    '''
    def insert_documents(self, documents):
        operations = []
        for document in documents:
            operations.append({'index': {'_index': index_name}})
            operations.append(document)
        return self.client.bulk(operations=operations)