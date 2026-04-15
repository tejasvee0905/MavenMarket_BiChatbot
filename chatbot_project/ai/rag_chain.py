import os
import re
import json
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document
from langchain_core.messages import HumanMessage, AIMessage
from ai.chart_builder import detect_chart

load_dotenv()

VECTORSTORE_DIR = os.path.join(os.path.dirname(__file__), "vectorstore")
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
KNOWLEDGE_DIR = os.path.join(PROJECT_ROOT, "knowledge", "active")

# ═══════════════════════════════════════════════════════════════
# ENHANCED SYSTEM PROMPT — with reasoning, few-shot, flexibility
# ═══════════════════════════════════════════════════════════════

SYSTEM_PROMPT = """You are MavenMarket BI Assistant — a smart, conversational AI that helps business users understand their Power BI dashboard data.

THINKING PROCESS:
1. Read the context carefully. Look for exact numbers, measure definitions, and insight summaries.
2. If the context contains the data, extract and present it clearly.
3. If the user asks a vague question, interpret it generously — if context has relevant data, use it.
4. If the context truly has no relevant information, respond EXACTLY with: "I don't have that information in the dashboard data." Do NOT guess, suggest DAX formulas, or fabricate answers.

RESPONSE RULES:
- Always provide actual numbers/data from the context when available.
- Format currency as $X,XXX.XX with commas.
- Format percentages with one decimal (e.g., 59.7%).
- Use markdown formatting: **bold** for key figures, bullet points for lists, tables for comparisons.
- When comparing items, present as a ranked table or ordered list.
- If asked about a DAX measure, show the formula AND explain in plain English.
- For "why" or analytical questions, reason from the available data (e.g., compare periods, segments).
- Support follow-up questions using conversation history for context.

CHART GENERATION:
When your answer contains comparative data (breakdowns, trends, rankings), present it as a markdown table. Charts will be auto-generated from your tables.

EXAMPLE INTERACTIONS:

User: What is the total revenue?
Assistant: **Total Revenue: $1,764,546.44**

This is calculated using the DAX measure `Total Revenue = SUMX(Transactions, quantity × retail_price)` across all 269,720 transactions.

User: How does that break down by country?
Assistant: Revenue by country:
| Country | Revenue | % of Total |
|---------|---------|-----------|
| **USA** | $1,177,956.44 | 66.8% |
| **Mexico** | $478,915.66 | 27.1% |
| **Canada** | $107,674.34 | 6.1% |

USA dominates with nearly two-thirds of total revenue.

User: What's our return rate?
Assistant: **Overall Return Rate: 1.0%** (8,289 returned units out of 833,489 sold)

Store 8 has the highest return rate at 1.2%, while Store 10 and Store 13 have the lowest at 0.9%.

CONTEXT FROM DASHBOARD:
{context}"""


# ═══════════════════════════════════════════════════════════════
# LOCAL QUERY EXPANSION (no API calls needed)
# ═══════════════════════════════════════════════════════════════

SYNONYM_MAP = {
    "revenue": ["sales", "income", "total revenue", "earnings"],
    "profit": ["profit margin", "total profit", "earnings", "net income"],
    "customer": ["customers", "buyer", "client", "total customers"],
    "store": ["stores", "shop", "location", "outlet"],
    "product": ["products", "item", "goods", "merchandise"],
    "return": ["returns", "returned", "return rate", "refund"],
    "country": ["countries", "region", "geography", "USA", "Mexico", "Canada"],
    "brand": ["brands", "manufacturer", "label"],
    "dashboard": ["report", "page", "visual", "chart"],
    "dax": ["measure", "formula", "expression", "calculated"],
    "top": ["highest", "best", "most", "leading", "number one"],
    "bottom": ["lowest", "worst", "least", "minimum"],
    "trend": ["growth", "change", "year-over-year", "monthly", "quarterly"],
    "weekend": ["saturday", "sunday", "weekday"],
    "gender": ["male", "female", "men", "women"],
    "income": ["salary", "yearly income", "income bracket", "earning"],
    "education": ["degree", "school", "college", "graduate"],
    "member": ["member card", "loyalty", "bronze", "silver", "golden"],
    "marital": ["married", "single", "marital status", "marriage"],
    "occupation": ["job", "profession", "work", "career"],
}


def expand_query(question: str) -> list[str]:
    """Generate alternative queries using local keyword expansion."""
    question_lower = question.lower()
    alt_queries = [question]

    for keyword, synonyms in SYNONYM_MAP.items():
        if keyword in question_lower:
            for syn in synonyms[:2]:
                alt = question_lower.replace(keyword, syn)
                if alt != question_lower:
                    alt_queries.append(alt)
                    break

    # Add a "data lookup" version
    alt_queries.append(f"Insight data about: {question}")

    return alt_queries[:4]  # max 4 queries


def _format_docs(docs):
    return "\n\n---\n\n".join(doc.page_content for doc in docs)


# ═══════════════════════════════════════════════════════════════
# KEYWORD FALLBACK RETRIEVER (no API calls needed)
# ═══════════════════════════════════════════════════════════════

def _load_all_knowledge_docs() -> list[Document]:
    """Load all knowledge documents from disk for keyword search fallback."""
    docs = []

    # Load insights
    insights_path = os.path.join(KNOWLEDGE_DIR, "insights.jsonl")
    if os.path.exists(insights_path):
        with open(insights_path) as f:
            for line in f:
                ins = json.loads(line.strip())
                docs.append(Document(
                    page_content=f"Insight — {ins['topic']}:\n{ins['insight']}",
                    metadata={"source": "insights", "topic": ins["topic"]}
                ))

    # Load measures
    measures_path = os.path.join(KNOWLEDGE_DIR, "measures.json")
    if os.path.exists(measures_path):
        with open(measures_path) as f:
            for m in json.load(f):
                docs.append(Document(
                    page_content=(
                        f"DAX Measure: {m['name']}\nTable: {m['table']}\n"
                        f"Expression: {m['expression']}\nDescription: {m['description']}\n"
                        f"Format: {m['format']}\nUsed in pages: {', '.join(m['used_in_pages'])}"
                    ),
                    metadata={"source": "measures", "topic": "dax_measure", "measure_name": m["name"]}
                ))

    # Load dashboard summary
    dashboard_path = os.path.join(KNOWLEDGE_DIR, "dashboard_summary.json")
    if os.path.exists(dashboard_path):
        with open(dashboard_path) as f:
            dashboard = json.load(f)
        docs.append(Document(
            page_content=f"Dashboard: {dashboard['report_name']}, {dashboard['total_pages']} pages, {dashboard['total_visuals']} visuals.",
            metadata={"source": "dashboard", "topic": "dashboard_overview"}
        ))
        for page in dashboard.get("pages", []):
            visuals_desc = "\n".join([f"  - {v['type']}: {v['shows']}" for v in page["visuals"]])
            docs.append(Document(
                page_content=f"Dashboard Page: {page['name']}\nPurpose: {page['purpose']}\nVisuals:\n{visuals_desc}",
                metadata={"source": "dashboard", "topic": "dashboard_page", "page": page["name"]}
            ))

    # Load schema
    schema_path = os.path.join(KNOWLEDGE_DIR, "schema.json")
    if os.path.exists(schema_path):
        with open(schema_path) as f:
            schema = json.load(f)
        for tname, tinfo in schema.get("tables", {}).items():
            cols_desc = "\n".join([f"  - {c}: {ci['type']} — {ci['description']}" for c, ci in tinfo.get("columns", {}).items()])
            docs.append(Document(
                page_content=f"Table: {tname}\nRole: {tinfo['role']}\nColumns:\n{cols_desc}",
                metadata={"source": "schema", "topic": "table_definition", "table": tname}
            ))

    return docs


def _keyword_search(docs: list[Document], query: str, k: int = 15) -> list[Document]:
    """Simple keyword-based retrieval — scores docs by word overlap with query."""
    query_words = set(re.findall(r'\w+', query.lower()))
    scored = []
    for doc in docs:
        doc_words = set(re.findall(r'\w+', doc.page_content.lower()))
        overlap = len(query_words & doc_words)
        # Bonus for exact phrase matches
        if query.lower() in doc.page_content.lower():
            overlap += 10
        # Bonus for topic match
        topic = doc.metadata.get("topic", "").lower()
        if any(w in topic for w in query_words):
            overlap += 5
        if overlap > 0:
            scored.append((overlap, doc))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [doc for _, doc in scored[:k]]


# ═══════════════════════════════════════════════════════════════
# PANDAS FALLBACK — generates and executes code against CSVs
# ═══════════════════════════════════════════════════════════════

def _get_pandas_engine():
    """Import pandas query engine (lazy to avoid circular imports)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "pandas_query",
        os.path.join(os.path.dirname(__file__), "pandas_query.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _try_pandas_fallback(llm, question: str, standalone_question: str) -> str | None:
    """Ask the LLM to generate pandas code, execute it, and format the result."""
    try:
        pq = _get_pandas_engine()
        schema = pq.get_schema_summary()

        code_prompt = f"""You have access to pandas DataFrames loaded from CSV files. Generate Python pandas code to answer this question:

Question: {standalone_question}

{schema}

RULES:
- Assign your final answer to a variable called `result`
- Use ONLY the DataFrames listed above (customers, products, stores, regions, transactions, returns, calendar, txn_full)
- Do NOT import anything, do NOT use print()
- Keep it simple — 1-5 lines of pandas code
- Return just the code, no markdown fences, no explanation

Code:"""

        code_response = llm.invoke([{"role": "user", "content": code_prompt}])
        code = code_response.content.strip()

        # Clean up markdown fences if present
        if code.startswith("```"):
            code = "\n".join(code.split("\n")[1:])
        if code.endswith("```"):
            code = "\n".join(code.split("\n")[:-1])
        code = code.strip()

        # Execute
        query_result = pq.execute_pandas_query(code)

        if query_result.startswith("Error"):
            return None

        # Format the result nicely using the LLM
        format_prompt = f"""The user asked: {question}

I queried the raw CSV data and got this result:
{query_result}

Format this into a clear, concise answer using markdown. Include the actual numbers. Be helpful and direct."""

        format_response = llm.invoke([{"role": "user", "content": format_prompt}])
        return format_response.content

    except Exception:
        return None


def _extract_chart(answer: str) -> tuple[dict | None, str]:
    """Extract chart JSON from answer text. Returns (chart_data, clean_answer)."""
    chart_pattern = r'```chart\s*\n(.*?)\n```'
    match = re.search(chart_pattern, answer, re.DOTALL)
    if not match:
        return None, answer

    try:
        chart_json = json.loads(match.group(1).strip())
        # Validate required fields
        if not all(k in chart_json for k in ("chart_type", "title", "data")):
            return None, answer
        if not isinstance(chart_json["data"], list) or len(chart_json["data"]) < 2:
            return None, answer
        # Remove chart block from answer text
        clean_answer = answer[:match.start()].rstrip()
        return chart_json, clean_answer
    except (json.JSONDecodeError, KeyError, TypeError):
        # If JSON is malformed, return answer without chart
        clean_answer = answer[:match.start()].rstrip()
        return None, clean_answer


def get_rag_chain(temperature=0.0, max_tokens=1024, top_p=1.0, top_k=15):
    """Build the RAG components with configurable parameters."""
    embeddings = OpenAIEmbeddings(
        model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
        base_url=os.getenv("BASE_URL", "https://models.inference.ai.azure.com"),
        api_key=os.getenv("GITHUB_TOKEN"),
    )

    vectorstore = FAISS.load_local(
        VECTORSTORE_DIR, embeddings, allow_dangerous_deserialization=True
    )
    retriever = vectorstore.as_retriever(search_kwargs={"k": top_k})

    llm = ChatOpenAI(
        model=os.getenv("LLM_MODEL", "gpt-4o"),
        base_url=os.getenv("BASE_URL", "https://models.inference.ai.azure.com"),
        api_key=os.getenv("GITHUB_TOKEN"),
        temperature=temperature,
        max_tokens=max_tokens,
        top_p=top_p,
    )

    return {
        "llm": llm,
        "retriever": retriever,
        "vectorstore": vectorstore,
        "knowledge_docs": _load_all_knowledge_docs(),
        "top_k": top_k,
    }


def ask_with_history(chain_components: dict, question: str, chat_history: list = None) -> dict:
    """Ask a question with conversation history and multi-query retrieval (no extra API calls)."""
    llm = chain_components["llm"]
    retriever = chain_components["retriever"]
    chat_history = chat_history or []

    # Step 1: Contextualize follow-ups using history (locally, no API call)
    standalone_question = question
    if chat_history:
        last_q, last_a = chat_history[-1]
        # Detect if this is a follow-up (short, or starts with "what about", "and", etc.)
        follow_up_patterns = [
            r"^what about\b", r"^how about\b", r"^and\b", r"^also\b",
            r"^what\'?s\b.*\?$", r"^that\b", r"^its?\b", r"^their\b",
        ]
        is_followup = len(question.split()) <= 8 or any(
            re.search(p, question.lower()) for p in follow_up_patterns
        )
        if is_followup:
            standalone_question = f"{question} (in context of the previous question: {last_q})"

    # Step 2: Multi-query retrieval using local keyword expansion
    all_queries = expand_query(standalone_question)

    seen_contents = set()
    all_docs = []
    faiss_failed = False
    for q in all_queries:
        try:
            docs = retriever.invoke(q)
            for doc in docs:
                content_hash = hash(doc.page_content)
                if content_hash not in seen_contents:
                    seen_contents.add(content_hash)
                    all_docs.append(doc)
        except Exception:
            faiss_failed = True
            break

    # Fallback: keyword search if FAISS embedding API is rate-limited
    if faiss_failed or not all_docs:
        knowledge_docs = chain_components.get("knowledge_docs", [])
        top_k = chain_components.get("top_k", 15)
        if knowledge_docs:
            for q in all_queries:
                kw_docs = _keyword_search(knowledge_docs, q, k=top_k)
                for doc in kw_docs:
                    content_hash = hash(doc.page_content)
                    if content_hash not in seen_contents:
                        seen_contents.add(content_hash)
                        all_docs.append(doc)

    # Step 3: Build prompt with context and history
    context = _format_docs(all_docs) if all_docs else "No specific data found for this query."

    messages = [{"role": "system", "content": SYSTEM_PROMPT.format(context=context)}]
    for human, ai in (chat_history or [])[-3:]:
        messages.append({"role": "user", "content": human})
        messages.append({"role": "assistant", "content": ai})
    messages.append({"role": "user", "content": question})

    # Step 4: Generate answer (single API call)
    response = llm.invoke(messages)
    answer = response.content

    # Collect sources
    sources = []
    for doc in all_docs:
        src = doc.metadata.get("source", "")
        topic = doc.metadata.get("topic", "")
        label = f"{src}: {topic}" if topic else src
        if label not in sources:
            sources.append(label)

    # Step 5: If answer indicates no data, try pandas fallback
    no_data_phrases = ["don't have", "not available", "no information", "not contain",
                       "cannot find", "don't know", "no specific data", "not include",
                       "outside the scope", "not in the dashboard"]
    if any(phrase in answer.lower() for phrase in no_data_phrases):
        pandas_result = _try_pandas_fallback(llm, question, standalone_question)
        if pandas_result:
            answer = pandas_result
            sources.append("live_csv_query")

    # Step 6: Extract chart data — try LLM-generated block first, then auto-detect
    chart_data, clean_answer = _extract_chart(answer)
    if chart_data is None:
        chart_data = detect_chart(answer, question)
        clean_answer = answer  # no block to remove

    return {
        "answer": clean_answer,
        "chart": chart_data,
        "sources": sources,
        "source_documents": all_docs,
        "standalone_question": standalone_question,
    }


# Backward compatible wrapper
def ask(question: str) -> dict:
    components = get_rag_chain()
    result = ask_with_history(components, question)
    return {
        "answer": result["answer"],
        "chart": result.get("chart"),
        "sources": result["sources"],
        "source_documents": result["source_documents"],
    }


if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "What is the total revenue?"
    result = ask(q)
    print(f"\nQ: {q}")
    print(f"\nA: {result['answer']}")
    print(f"\nSources: {result['sources']}")
