from fileinput import filename
from datetime import datetime, timezone
import os
from opensearchpy import helpers
import pymupdf
from docx import Document
from unstructured.partition.docx import partition_docx
from unstructured.chunking.basic import chunk_elements
from langchain_text_splitters import RecursiveCharacterTextSplitter
from opensearch import Search

#Model ID: -JcaUJoBSg4Qb7bxz4UP

search = Search()

index_name = 'test'

# model_id = search.client.ml.register_model(
#     body={
#         "name": "huggingface/sentence-transformers/msmarco-distilbert-base-tas-b",
#         "version": "1.0.3",
#         "model_group_id": "6ZcaUJoBSg4Qb7bxcYUw",
#         "model_format": "TORCH_SCRIPT"
#     }
# )
# print(model_id)

docs = [{
  "passage_text": "testing testing 123 keywords keywords blueberry apple dog wolf",
  "path": "/home/bome/yeah",
  "last_edit": "2001-01-01",
  "page": 4
}]

helpers.bulk(search.client, docs, index=index_name, pipeline="nlp-ingest-pipeline",request_timeout=120)
print("yeah")