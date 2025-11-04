import re
import markdown
from flask import Flask, render_template, request
from opensearch import Search
from langchain_ollama.llms import OllamaLLM


app = Flask(__name__)
search = Search()
llm = OllamaLLM(model="gpt-oss:20b")

@app.get('/')
def index():
    return render_template('index.html')

@app.post('/')
def handle_search():
    query = request.form.get('query', '')
    #heres where the search happens
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
                        "query_text": query,
                        "model_id": "-JcaUJoBSg4Qb7bxz4UP",
                        "k": 100
                    }
                }
            }
        }
    )
    ai_reply = rag("summarize this", results)
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
    return llm.invoke(f"{user_request}\n Context: {concatenated}")
