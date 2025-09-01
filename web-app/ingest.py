from fileinput import filename
import os
import pymupdf
from langchain.text_splitter import RecursiveCharacterTextSplitter
from search import Search

es = Search()

es.create_index()

index_name = "semantic_test"
mappings = {
    "properties": {
        "text": {
            "type": "semantic_text"
        }
    }
}


mapping_response = es.es.indices.put_mapping(index=index_name, body=mappings)
print(mapping_response)