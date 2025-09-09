from fileinput import filename
from datetime import datetime, timezone
import os
from elasticsearch import Elasticsearch, helpers
import pymupdf
from docx import Document
from unstructured.partition.docx import partition_docx
from unstructured.chunking.basic import chunk_elements
from langchain.text_splitter import RecursiveCharacterTextSplitter

client = Elasticsearch(
    "http://localhost:9200",
    api_key="UGswNm01Z0Iwc3paRThSdlJYM0k6U3JMREN2UjVfYWozZ1YyZXVXZjBldw==",
)

index_name = "semantic_test"

splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100, length_function=len)

def insert_documents(docs):
    return helpers.bulk(client.options(request_timeout=300), docs, index=index_name)

def process_file(file_name, curr_time, log, file_path = './database/'):
    if file_name.endswith(".pdf"):
        try:
            return process_pdf(file_name, curr_time, file_path)
        except Exception as e:
            print(f"Skipping {file_path}: {e}")
            return []
    elif file_name.endswith(".docx"):
        try:
            return process_docx(file_name, curr_time, file_path)
        except Exception as e:
            print(f"Skipping {file_path}: {e}")
            log.write(f"Error processing {file_path}: {e}\n")
            return []
    return []

def process_pdf(file_name, curr_time, file_path = './database/'):
    file_path = os.path.join(file_path, file_name)
    pages = pymupdf.open(file_path)

    documents = []
    for page in pages:
        page_number = page.number + 1
        doc_text = splitter.split_text(page.get_text())
        for chunk in doc_text:
            documents.append({
                "title": file_path,
                "text": chunk,
                "page": page_number,
                "last_edit_date": curr_time
            })
    return documents

def process_docx(file_name, curr_time, file_path = './database/'):
    file_path = os.path.join(file_path, file_name)
    pages = partition_docx(file_path)
    basic_chunks = chunk_elements(pages, max_characters=1000)
    documents = []
    for chunk in basic_chunks:
        documents.append({
            "title": file_path,
            "text": chunk.text,
            "paragraph": "N/A",
            "last_edit_date": curr_time
        })
    return documents

def ingest_tree(folder_path = './database/temp/'):
    all_documents = []
    date = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    f = open(f"log_{datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")}.txt", "a")
    for root, subdirs, files in os.walk(folder_path):
        for filename in files:
            all_documents.extend(process_file(filename, date, f, root))

    f.close()
    return all_documents

#print(process_docx("40 72 13 Ultrasonic Level Transmitter.docx", datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")))
#insert_documents(process_pdf("SUB-27R1-Silt Fence Package.pdf", datetime.datetime.now()))
insert_documents(ingest_tree())