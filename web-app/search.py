import json
from pprint import pprint
import os
import time

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()
PORT = os.getenv("PORT")
API_KEY = os.getenv("API_KEY")
index_name = os.getenv("INDEX_NAME")
#remove overwrite test name later
index_name = "semantic_test"

class Search:
    def __init__(self):
        self.es = Elasticsearch(f'http://localhost:{PORT}', api_key=API_KEY)
        client_info = self.es.info()
        print('Connected to Elasticsearch!')
        pprint(client_info.body)

    def create_index(self):
        self.es.indices.delete(index=index_name, ignore_unavailable=True)
        self.es.indices.create(index=index_name)

    def reindex(self):
        self.create_index()
        with open('data.json', 'rt') as f:
            documents = json.loads(f.read())
        return self.insert_documents(documents)
    
    def search(self, **query_args):
        return self.es.search(index=index_name, **query_args)

    '''
    Insert several documents
    '''
    def insert_document(self, document):
        return self.es.index(index=index_name, body=document)

    '''
    Bulk insert documents with 1 api call
    '''
    def insert_documents(self, documents):
        operations = []
        for document in documents:
            operations.append({'index': {'_index': index_name}})
            operations.append(document)
        return self.es.bulk(operations=operations)