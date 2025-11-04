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

index_name = 'eto_search'

# model_id = search.client.ml.register_model(
#     body={
#         "name": "huggingface/sentence-transformers/msmarco-distilbert-base-tas-b",
#         "version": "1.0.3",
#         "model_group_id": "6ZcaUJoBSg4Qb7bxcYUw",
#         "model_format": "TORCH_SCRIPT"
#     }
# )
# print(model_id)

