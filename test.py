from fileinput import filename
import datetime
import os
from elasticsearch import Elasticsearch, helpers
import pymupdf
from docx import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter

client = Elasticsearch(
    "http://localhost:9200",
    api_key="UGswNm01Z0Iwc3paRThSdlJYM0k6U3JMREN2UjVfYWozZ1YyZXVXZjBldw==",
)

splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100, length_function=len)

index_name = "semantic_test"

def insert_documents(docs):
    return helpers.bulk(client.options(request_timeout=300), docs, index=index_name)

def process_file(file_name, log, file_path = './database/'):
    if file_name.endswith(".pdf"):
        try:
            return process_pdf(file_name, file_path)
        except Exception as e:
            print(f"Skipping {file_path}: {e}")
            return []
    elif file_name.endswith(".docx"):
        try:
            return process_docx(file_name, file_path)
        except Exception as e:
            print(f"Skipping {file_path}: {e}")
            log.write(f"Error processing {file_path}: {e}\n")
            return []
    return []

def process_pdf(file_name, file_path = './database/'):
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
                "page": page_number
            })
    return documents

def process_docx(file_name, file_path = './database/'):
    file_path = os.path.join(file_path, file_name)
    pages = Document(file_path)

    documents = []
    for i, para in enumerate(pages.paragraphs):
        doc_text = splitter.split_text(para.text)
        for chunk in doc_text:
            documents.append({
                "title": file_path,
                "text": chunk,
                "paragraph": i + 1
            })
    return documents

def ingest_tree(folder_path = './database/temp/'):
    all_documents = []
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    time = datetime.datetime.now().strftime("%H-%M-%S")
    f = open(f"log_{date}_{time}.txt", "a")
    for root, subdirs, files in os.walk(folder_path):
        for filename in files:
            all_documents.extend(process_file(filename, f, root))

    f.close()
    return all_documents

#print(process_docx("40 72 13 Ultrasonic Level Transmitter.docx"))
#insert_documents(process_pdf("SUB-27R1-Silt Fence Package.pdf"))
insert_documents(ingest_tree())