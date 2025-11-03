from fileinput import filename
from datetime import datetime, timezone
import os
from opensearchpy import helpers
import pymupdf
from docx import Document
from unstructured.partition.docx import partition_docx
from unstructured.chunking.basic import chunk_elements
from langchain.text_splitter import RecursiveCharacterTextSplitter
from opensearch import Search

search = Search()

index_name = 'python-test-index'
index_body = {
  'settings': {
    'index': {
      'number_of_shards': 4
    }
  }
}

document = {
  'title': 'Moneyball',
  'director': 'Bennett Miller',
  'year': '2011'
}

response = search.client.index(
    index = 'python-test-index',
    body = document,
    id = '1',
    refresh = True
)

model_id = search.client.ml.register_model(
    body={
        "name": "huggingface/sentence-transformers/msmarco-distilbert-base-tas-b",
        "version": "1.0.3",
        "model_group_id": "Z1eQf4oB5Vm0Tdw8EIP2",
        "model_format": "TORCH_SCRIPT"
    }
)
print(model_id)