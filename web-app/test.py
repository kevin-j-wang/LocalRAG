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

os = Search()

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

response = os.client.index(
    index = 'python-test-index',
    body = document,
    id = '1',
    refresh = True
)