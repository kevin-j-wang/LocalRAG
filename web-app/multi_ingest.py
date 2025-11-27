from fileinput import filename
from datetime import datetime, timezone
import os
from opensearchpy import helpers
import pymupdf
from docx import Document
from unstructured.partition.docx import partition_docx
from unstructured.partition.pdf import partition_pdf
from unstructured.chunking.basic import chunk_elements
from langchain_text_splitters import RecursiveCharacterTextSplitter
from opensearch import Search
from timeout import timeout
from multiprocessing import Process, Queue

client = Search()

index_name = os.getenv("INDEX_NAME")
mappings = {
    "properties": {
        "passage_text": {
            "type": "text"
        },
        "date_uploaded": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis"
        },
        "path": {
            "type": "text"
        },
        "page": {
            "type": "integer"
        }
    }
}

def insert_documents(docs):
    print(f"inserting batch of size {len(docs)}")
    return helpers.bulk(client.client, docs, index=index_name, pipeline="nlp-ingest-pipeline",request_timeout=120)

def process_file(file_name, log, file_path = './database/'):
    if file_name.endswith(".pdf"):
        try:
            return process_pdf_unstructured(file_name, file_path)
        except Exception as e:
            print(f"Skipping {file_path}: {e}")
            log.write(f"Error processing {file_path}: {e}\n")
            return []
    # elif file_name.endswith(".docx"):
    #     try:
    #         return process_docx(file_name, curr_time, file_path)
    #     except Exception as e:
    #         print(f"Skipping {file_path}: {e}")
    #         log.write(f"Error processing {file_path}: {e}\n")
    #         return []
    return []

def process_pdf(file_name, file_path = './database/'):
    splitter = RecursiveCharacterTextSplitter(chunk_size=2048, chunk_overlap=100, length_function=len)
    file_path = os.path.join(file_path, file_name)
    pages = pymupdf.open(file_path)
    documents = []
    timestamp = os.path.getmtime(file_path)
    last_modified = datetime.fromtimestamp(timestamp)
    for page in pages:
        page_number = page.number + 1
        doc_text = splitter.split_text(page.get_text())
        for chunk in doc_text:
            documents.append({
                "path": file_path,
                "passage_text": chunk,
                "page": page_number,
                "last_edit": last_modified
            })
    print(f"Successfully chunked {file_path}\n")
    return documents

@timeout(90)
def process_pdf_unstructured(file_name, file_path = './database/'):
    file_path = os.path.join(file_path, file_name)
    documents = []
    timestamp = os.path.getmtime(file_path)
    last_modified = datetime.fromtimestamp(timestamp).isoformat()
    ids = check_update(file_path, last_modified)
    #DELETE OLD
    if(ids):
        pages = partition_pdf(filename=file_path, languages=["eng"], strategy="auto")
        basic_chunks = chunk_elements(pages, max_characters = 2048, overlap = 100)
        if ids[0] == 'new':
            for chunk in basic_chunks:
                documents.append({
                    "path": file_path,
                    "passage_text": chunk.text,
                    "last_edit": last_modified
                })
            print(f"Successfully chunked {file_path}\n")
            return documents
        else:
            print("Deleting outdated documents.")
            for id in ids:
                client.delete(document_id=id)
            for chunk in basic_chunks:
                documents.append({
                    "path": file_path,
                    "passage_text": chunk.text,
                    "last_edit": last_modified
                })
            print(f"Successfully chunked {file_path}\n")
            return documents
    else:
        return []

def process_docx(file_name, file_path = './database/'):
    file_path = os.path.join(file_path, file_name)
    pages = partition_docx(file_path)
    basic_chunks = chunk_elements(pages, max_characters=2048, overlap = 100)
    documents = []
    timestamp = os.path.getmtime(file_path)
    last_modified = datetime.fromtimestamp(timestamp)
    ids = check_update(file_path, last_modified)
    for chunk in basic_chunks:
        documents.append({
            "title": file_path,
            "text": chunk.text,
            "paragraph": "N/A",
            "last_edit": last_modified
        })
    return documents

def process_tree(folder_path = './database/temp/'):
    all_documents = []
    f = open(f"log_{datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")}.txt", "a")
    for root, subdirs, files in os.walk(folder_path):
        for filename in files:
            all_documents.extend(process_file(filename, f, root))

    f.close()
    return all_documents

def handle_batches(docs, batch_size = 1000):
    if len(docs) < batch_size:
        insert_documents(docs[0:])
        return
    l = 0
    r = l + batch_size
    while r < len(docs):
        insert_documents(docs[l:r])
        l += batch_size
        r += batch_size
    if l < len(docs) and r > len(docs):
        insert_documents(docs[l:])

def handle_batches_multi(q):
    while True:
        print("Checking queue")
        try:
            data = q.get()
            if data == 'END':
                print("COMPLETE")
                break
            insert_documents(data)
            print("finished inserting")
        except KeyboardInterrupt:
            print("Manual Exiting")
            exit()
        except:
            print("waiting...")

def prepare_batches(q, path):
    f = open(f"log_{datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")}.txt", "a")
    batch = []
    IGNORE_FOLDERS = {'#recycle', '#snapshot', '00000 (Folder Stru)', '00 PENDING'}
    for root, subdirs, files in os.walk(path):
        subdirs[:] = [
            d for d in subdirs
            if d not in IGNORE_FOLDERS
        ]

        for filename in files:
            batch.extend(process_file(filename, f, root))
        if len(batch) >= 250:
            q.put(batch)
            batch = []

    q.put(batch)
    q.put('END')
    print("putting end")
    f.close()

def check_update(path, curr_edit):
    last_updated = client.search(
        body={
            "_source": "last_edit",
            "query": {
                "term": {
                    "path": path
                }
            }
        }
    )
    results = last_updated['hits']['hits']
    if len(results) == 0:
        print("new file")
        return ['new']
    ids = []
    for doc in results:
        if doc['_source'].get('last_edit') == curr_edit:
            print("No edit needed")
            return []
        else:
            ids.append(doc['_id'])
    return ids

if __name__ == "__main__":
    q = Queue()
    path = '/Volumes/Projects'
    p = Process(target=prepare_batches, args=(q, path))
    try:
        p.start()
    except KeyboardInterrupt:
        p.terminate()
        p.join()
    handle_batches_multi(q)