from dotenv import load_dotenv
from langchain_ollama.llms import OllamaLLM

from langchain_community.document_loaders import PyPDFLoader
import pymupdf

import getpass
import os
from langchain_core.vectorstores import InMemoryVectorStore
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.documents import Document

from langchain_ollama import OllamaEmbeddings
from langchain_chroma import Chroma


def load_all_pdfs_from_folder(folder_path):
    all_documents = []

    for filename in os.listdir(folder_path):
        if filename.endswith(".pdf"):
            file_path = os.path.join(folder_path, filename)
            loader = pymupdf.open(file_path)
            doc_text = "\n".join(page.get_text() for page in loader)
            documents = Document(page_content = doc_text, metadata={"source": file_path})
            all_documents.append(documents)
    return all_documents

def load_pdf(path):
    loader = pymupdf.open(path)
    doc_text = "\n".join(page.get_text() for page in loader)
    return [Document(page_content=doc_text, metadata={"source": path})]

embedding_model = OllamaEmbeddings(model="nomic-embed-text")

#other model gpt-oss:20b
#other other model llama3.2
llm = OllamaLLM(model="llama3.2")

folder_path = "C:\\Users\\Kevin\\Documents\\Work\\RAG\\database\\15200 High Lift Pump Relief Piping Shop Drawing.pdf"
documents = load_pdf(folder_path)
# documents = load_all_pdfs_from_folder(folder_path)
splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=100)
documents = splitter.split_documents(documents)
db = Chroma.from_documents(documents, embedding_model, persist_directory="./chroma_db")
db = Chroma(persist_directory="./chroma_db", embedding_function=embedding_model)

query1 = input("Enter context: ")

results = db.similarity_search(query1, k=1)
for r in results:
    print(r.page_content)
context = "\n".join([r.page_content for r in results])

query2 = input("Enter question: ")

query = f"{query2}: {context}"

response = llm.invoke(query)
print(response)