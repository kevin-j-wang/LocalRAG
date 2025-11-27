import re
import markdown
from flask import Flask, render_template, request
from opensearch import Search
from langchain_ollama.llms import OllamaLLM

app = Flask(__name__)
search = Search()
llm = OllamaLLM(model="gemma3:12b")
queryLLM = OllamaLLM(model="qwen3:8b")

@app.get('/')
def index():
    return render_template('index.html')

@app.post('/')
def handle_search():
    query = request.form.get('query', '')
    #heres where the search happens
    search_query = queryLLM.invoke(f"You have access to a database of information that can be searched semantically. Given the following user query, generate a single search query that will enable you to retrieve the information required to reply accurately. Do not generate rationale or any additional information that could cloud the search. {query}")
    print(search_query)
    results = search.search(
        body={
            "_source": {
                "excludes": [
                    "passage_embedding"
                ]
            },
            "query": {
                "neural": {
                    "passage_embedding": {
                        "query_text": search_query,
                        "model_id": "-JcaUJoBSg4Qb7bxz4UP",
                        "k": 20
                    }
                }
            }
        }
    )
    ai_reply = rag(query, results)
    return render_template('index.html', results=results['hits']['hits'], ai_reply=ai_reply, query=query, from_=0, total=results['hits']['total']['value'])

@app.get('/document/<id>')
def get_document(id):
    return 'Document not found'

@app.cli.command()
def reindex():
    response = search.reindex()
    print(f'Index with {len(response["items"])} documents created '
          f'in {response["took"]} milliseconds.')
    
@app.template_filter("markdown")
def markdown_filter(text):
    return markdown.markdown(text)

def rag(user_request, results):
    top_n = 10
    relevant_docs = results['hits']['hits'][:top_n]
    concatenated = ""
    for x in relevant_docs:
        concatenated += f"Title: {x['_source']['path']}\n"
        concatenated += f"Text: {x['_source']['passage_text']}\n"
    return llm.invoke(f"Drawing from the below information, answer the following query: {user_request}\n Context: {concatenated}. Make sure to cite your sources from the context provided.")
