# AI-Powered Power BI Chatbot — Corporate Project Framework

> **Project Name:** MavenMarket Intelligent BI Assistant  
> **Version:** 1.0  
> **Date:** April 2026  
> **Audience:** Corporate BI Team, Data Engineering, IT Leadership

---

## 1. Executive Summary

This framework defines an end-to-end system that:

1. **Ingests** the Power BI semantic model (data, DAX measures, relationships, visuals)
2. **Extracts knowledge** — schema metadata, pre-computed insights, KPI definitions, dashboard visual context
3. **Embeds** all knowledge into a vector store for semantic retrieval
4. **Deploys a chatbot** that lets business users ask natural-language questions and get answers grounded in the actual dashboard data

The chatbot does **not** hallucinate — every answer is backed by the real data model and dashboard artifacts via Retrieval-Augmented Generation (RAG).

---

## 2. End-to-End Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         1. DATA LAYER                                       │
│  CSV Sources ──► Power BI Semantic Model (Star Schema + DAX Measures)       │
└────────────────────────────┬────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    2. KNOWLEDGE EXTRACTION LAYER                            │
│  Schema Extractor │ DAX Catalog │ Visual Parser │ Pre-computed Insights     │
└────────────────────────────┬────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       3. AI / LLM LAYER                                     │
│  Embedding Pipeline ──► Vector Store ──► RAG Chain ◄── LLM (GPT-4o/4.1)   │
│                                          NL-to-DAX / NL-to-SQL Agent        │
└────────────────────────────┬────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     4. CHATBOT INTERFACE                                     │
│  MS Teams Bot / Web App  │  Azure AD Auth  │  Content Safety Guardrails     │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Model — MavenMarket Star Schema

### 3.1 Tables & Row Counts

| Table          | Rows     | Role          | Grain                 |
|----------------|----------|---------------|-----------------------|
| Transactions   | 269,720  | Fact          | One row per line item |
| Returns        | 7,087    | Fact          | One row per return    |
| Customers      | 10,281   | Dimension     | One row per customer  |
| Products       | 1,560    | Dimension     | One row per product   |
| Stores         | 24       | Dimension     | One row per store     |
| Regions        | 109      | Dimension     | One row per region    |
| Calendar       | 730      | Dimension     | One row per date      |

### 3.2 Relationships (Star Schema Keys)

| From Table     | Key               | To Table      | Cardinality |
|----------------|-------------------|---------------|-------------|
| Transactions   | transaction_date  | Calendar      | Many → One  |
| Transactions   | customer_id       | Customers     | Many → One  |
| Transactions   | product_id        | Products      | Many → One  |
| Transactions   | store_id          | Stores        | Many → One  |
| Returns        | return_date       | Calendar      | Many → One  |
| Returns        | product_id        | Products      | Many → One  |
| Returns        | store_id          | Stores        | Many → One  |
| Stores         | region_id         | Regions       | Many → One  |

### 3.3 Recommended DAX Measures (to build in Power BI)

```dax
// Revenue
Total Revenue = SUMX(Transactions, Transactions[quantity] * RELATED(Products[product_retail_price]))

// Cost
Total Cost = SUMX(Transactions, Transactions[quantity] * RELATED(Products[product_cost]))

// Profit
Total Profit = [Total Revenue] - [Total Cost]

// Profit Margin %
Profit Margin % = DIVIDE([Total Profit], [Total Revenue], 0)

// Total Transactions
Total Transactions = COUNTROWS(Transactions)

// Total Customers
Total Customers = DISTINCTCOUNT(Transactions[customer_id])

// Total Returns
Total Returns = SUM(Returns[quantity])

// Return Rate
Return Rate = DIVIDE([Total Returns], SUM(Transactions[quantity]), 0)

// Revenue Previous Month
Revenue PM = CALCULATE([Total Revenue], DATEADD(Calendar[transaction_date], -1, MONTH))

// Revenue MoM Growth %
Revenue MoM % = DIVIDE([Total Revenue] - [Revenue PM], [Revenue PM], 0)
```

---

## 4. Phase-by-Phase Implementation

### Phase 1 — Foundation (Weeks 1–3)

**Goal:** Build & publish the Power BI dashboard  

| Task | Owner | Deliverable |
|------|-------|-------------|
| Load CSVs into Power BI Desktop | BI Developer | .pbix file |
| Build star-schema relationships | BI Developer | Data model |
| Create DAX measures (see §3.3) | BI Developer | Measure table |
| Design dashboard pages (Executive Summary, Store Performance, Product Analysis, Customer Segments) | BI Developer | 4 report pages |
| Publish to Power BI Service workspace | BI Admin | Published dataset + report |
| Enable XMLA read endpoint on Premium/PPU workspace | BI Admin | Endpoint URL |

### Phase 2 — Knowledge Extraction (Weeks 4–5)

**Goal:** Extract everything the AI needs to know about the BI model  

#### 2A. Schema & Metadata Extraction

Use the **Tabular Object Model (TOM)** or **Semantic Link** in Python:

```python
# Option A — semantic-link (Fabric native)
import sempy.fabric as fabric

df_model = fabric.list_datasets(workspace="MavenMarket_Workspace")
tables   = fabric.list_tables(dataset="MavenMarket", workspace="MavenMarket_Workspace")
measures = fabric.list_measures(dataset="MavenMarket", workspace="MavenMarket_Workspace")
relationships = fabric.list_relationships(dataset="MavenMarket", workspace="MavenMarket_Workspace")

# Option B — pyadomd + XMLA endpoint
from pyadomd import Pyadomd
conn_str = "Provider=MSOLAP;Data Source=powerbi://api.powerbi.com/v1.0/myorg/MavenMarket_Workspace;..."
with Pyadomd(conn_str) as conn:
    conn.cursor().execute("SELECT * FROM $SYSTEM.TMSCHEMA_TABLES")
```

**Output:** JSON files → `schema.json`, `measures.json`, `relationships.json`

#### 2B. Dashboard Visual Metadata

Export visual definitions from the .pbix using **pbi-tools** (open source):

```bash
pbi-tools extract MavenMarket.pbix -outdir ./pbi-extracted
```

This produces JSON for every visual (chart type, fields used, filters, drill-down paths).

#### 2C. Pre-computed Insights (Python)

Run analytics on the raw data to generate natural-language insight snippets:

```python
import pandas as pd

txn = pd.read_csv("MavenMarket_Transactions.csv", parse_dates=["transaction_date"], dayfirst=True)
products = pd.read_csv("MavenMarket_Products.csv")
stores = pd.read_csv("MavenMarket_Stores.csv")
customers = pd.read_csv("MavenMarket_Customers.csv")
regions = pd.read_csv("MavenMarket_Regions.csv")
returns = pd.read_csv("MavenMarket_Returns.csv", parse_dates=["return_date"], dayfirst=True)

# Merge for revenue
txn_enriched = txn.merge(products, on="product_id")
txn_enriched["revenue"] = txn_enriched["quantity"] * txn_enriched["product_retail_price"]
txn_enriched["profit"]  = txn_enriched["quantity"] * txn_enriched["price_margin"]

# ---------- Insight: Top 10 Products by Revenue ----------
top_products = (txn_enriched.groupby("product_name")["revenue"]
                .sum().sort_values(ascending=False).head(10))

# ---------- Insight: Monthly Revenue Trend ----------
monthly_rev = (txn_enriched.set_index("transaction_date")
               .resample("M")["revenue"].sum())

# ---------- Insight: Return Rate by Store ----------
store_returns = returns.groupby("store_id")["quantity"].sum()
store_sales   = txn.groupby("store_id")["quantity"].sum()
return_rate   = (store_returns / store_sales).sort_values(ascending=False)

# ---------- Insight: Customer Segments by Income ----------
cust_revenue = (txn_enriched.merge(customers, on="customer_id")
                .groupby("yearly_income")["revenue"].sum()
                .sort_values(ascending=False))

# ... Generate 50-100 such insight snippets as text paragraphs
# Save as insights.jsonl (one JSON object per insight)
```

**Output:** `insights.jsonl` — each line is `{"topic": "...", "insight": "...", "data": {...}}`

### Phase 3 — AI Backend (Weeks 6–9)

**Goal:** Build the RAG pipeline that answers questions using BI knowledge  

#### 3A. Technology Stack

| Component | Recommended Option | Alternative |
|-----------|--------------------|-------------|
| LLM | Azure OpenAI GPT-4o / GPT-4.1 | OpenAI API direct |
| Embeddings | text-embedding-ada-002 / text-embedding-3-large | Sentence Transformers (local) |
| Vector Store | Azure AI Search | Pinecone / FAISS (local) / ChromaDB |
| Orchestration | LangChain / Semantic Kernel | LlamaIndex |
| NL-to-DAX | Custom agent with few-shot prompting | Fabric Copilot API |

#### 3B. Embedding Pipeline

```python
from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import AzureSearch
from langchain.text_splitter import RecursiveCharacterTextSplitter
import json

# Load all knowledge documents
docs = []
docs += load_json_as_docs("schema.json", source="schema")
docs += load_json_as_docs("measures.json", source="dax_measures")
docs += load_json_as_docs("relationships.json", source="relationships")
docs += load_jsonl_as_docs("insights.jsonl", source="insights")
docs += load_json_as_docs("visual_metadata.json", source="dashboard_visuals")

# Chunk
splitter = RecursiveCharacterTextSplitter(chunk_size=800, chunk_overlap=100)
chunks = splitter.split_documents(docs)

# Embed & index
embeddings = AzureOpenAIEmbeddings(
    azure_deployment="text-embedding-ada-002",
    azure_endpoint="https://<your-resource>.openai.azure.com/"
)
vectorstore = AzureSearch(
    azure_search_endpoint="https://<your-search>.search.windows.net",
    index_name="maven-market-knowledge",
    embedding_function=embeddings.embed_query
)
vectorstore.add_documents(chunks)
```

#### 3C. RAG Chain

```python
from langchain_openai import AzureChatOpenAI
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate

llm = AzureChatOpenAI(
    azure_deployment="gpt-4o",
    temperature=0,
    azure_endpoint="https://<your-resource>.openai.azure.com/"
)

SYSTEM_PROMPT = """You are MavenMarket BI Assistant. You answer business questions
using ONLY the provided context from the Power BI dashboard and data model.

Rules:
- If the context doesn't contain the answer, say "I don't have that data."
- Always cite the source (measure name, table, or insight ID).
- Format numbers with commas and 2 decimal places for currency.
- When asked about trends, include the time period.

Context:
{context}

Question: {question}
Answer:"""

qa_chain = RetrievalQA.from_chain_type(
    llm=llm,
    retriever=vectorstore.as_retriever(search_kwargs={"k": 8}),
    chain_type_kwargs={"prompt": PromptTemplate.from_template(SYSTEM_PROMPT)},
    return_source_documents=True
)
```

#### 3D. NL-to-DAX Agent (Advanced Feature)

For live queries against the Power BI model:

```python
from langchain.agents import Tool, AgentExecutor, create_react_agent

def execute_dax(query: str) -> str:
    """Execute a DAX query against the Power BI XMLA endpoint and return results."""
    from pyadomd import Pyadomd
    with Pyadomd(XMLA_CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return str(cursor.fetchall())

dax_tool = Tool(
    name="ExecuteDAX",
    func=execute_dax,
    description="Executes a DAX query against the MavenMarket Power BI dataset. "
                "Input must be a valid DAX EVALUATE statement."
)

NL2DAX_PROMPT = """You translate natural language to DAX queries for the MavenMarket model.

Available tables & columns:
{schema_summary}

Available measures:
{measures_summary}

User question: {input}
Think step by step, then write a DAX EVALUATE statement.
"""

agent = create_react_agent(llm, [dax_tool], NL2DAX_PROMPT)
agent_executor = AgentExecutor(agent=agent, tools=[dax_tool], verbose=True)
```

### Phase 4 — Chatbot Interface (Weeks 10–12)

**Goal:** User-facing chat UI integrated with corporate infrastructure  

#### Option A — Microsoft Teams Bot (Recommended for Corporate)

```
Azure Bot Service → Bot Framework SDK (Python/C#) → RAG Chain → Response
```

- Users ask questions in a Teams channel or 1:1 chat
- Azure AD SSO for authentication (no separate login)
- Adaptive Cards for rich formatted responses with charts

#### Option B — Web App (Streamlit / React)

```python
# Streamlit prototype (quickest to demo)
import streamlit as st

st.title("MavenMarket BI Assistant")

if question := st.chat_input("Ask about the dashboard..."):
    with st.spinner("Analyzing..."):
        result = qa_chain.invoke({"query": question})
    st.write(result["result"])
    with st.expander("Sources"):
        for doc in result["source_documents"]:
            st.write(f"- [{doc.metadata['source']}] {doc.page_content[:200]}...")
```

#### Security & Guardrails

| Control | Implementation |
|---------|---------------|
| Authentication | Azure AD / Entra ID SSO |
| Row-Level Security | Pass user identity → PBI RLS roles |
| Content Safety | Azure AI Content Safety API |
| PII Filtering | Presidio (Microsoft open-source) |
| Query Limits | Rate limiting per user (10 req/min) |
| Audit Logging | Log every query + response to Azure Monitor |

### Phase 5 — Launch & Iteration (Week 13+)

| Task | Details |
|------|---------|
| Pilot with 10–15 power users | Collect feedback on answer quality |
| Tune retrieval (k, chunk size, reranking) | Improve answer relevance |
| Add feedback loop (thumbs up/down) | Store in DB for fine-tuning |
| Expand to more dashboards | Re-run Phase 2 extraction for each new report |
| Fine-tune or use GPT-4.1 for DAX generation | Improve NL-to-DAX accuracy |

---

## 5. Sample User Interactions

| User Question | Chatbot Behavior | Response Type |
|---------------|-------------------|---------------|
| "What was total revenue last quarter?" | RAG retrieves pre-computed insight | Text answer |
| "Which store has the highest return rate?" | RAG retrieves insight | Text + table |
| "Show me monthly revenue trend" | RAG returns insight, bot formats chart | Adaptive Card / chart |
| "What's revenue for Washington brand products in Mexico?" | NL-to-DAX agent generates & executes query | Live data answer |
| "Why did profit drop in March?" | RAG retrieves anomaly insight + related measures | Analytical narrative |
| "What does the Profit Margin % measure calculate?" | RAG retrieves DAX definition from measure catalog | DAX formula + explanation |

---

## 6. Technology Stack Summary

| Layer | Technology | License |
|-------|-----------|---------|
| BI Platform | Power BI Premium / PPU | Microsoft |
| Data Source | CSV → Power BI Dataflow or Direct Import | — |
| Knowledge Extraction | Python (semantic-link, pbi-tools, pandas) | Open Source |
| LLM | Azure OpenAI Service (GPT-4o / GPT-4.1) | Pay-per-token |
| Embeddings | text-embedding-ada-002 | Pay-per-token |
| Vector Store | Azure AI Search | Azure subscription |
| Orchestration | LangChain 0.3+ / Semantic Kernel | Open Source |
| Chatbot UI | Microsoft Bot Framework + Teams | Microsoft 365 |
| Auth | Azure AD / Entra ID | Microsoft 365 |
| Monitoring | Azure Monitor + Application Insights | Azure subscription |

---

## 7. Cost Estimation (Monthly, Approximate)

| Component | Estimated Cost |
|-----------|---------------|
| Azure OpenAI (GPT-4o, ~100K tokens/day) | $200–400 |
| Azure OpenAI Embeddings (indexing + queries) | $20–50 |
| Azure AI Search (Basic tier) | $75 |
| Azure Bot Service (Standard) | $50 |
| Azure App Service (chatbot hosting) | $50–100 |
| Power BI Premium Per User (existing) | Already budgeted |
| **Total** | **~$400–700/month** |

---

## 8. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| LLM hallucination | Users get wrong numbers | Strict RAG grounding + "I don't know" fallback |
| Stale insights | Dashboard updated but vector store isn't | Nightly refresh pipeline |
| XMLA endpoint unavailable (non-Premium) | Can't use NL-to-DAX live queries | Fall back to pre-computed insights only |
| Data sensitivity | Chatbot exposes restricted data | Azure AD + Power BI RLS passthrough |
| Token cost spike | Unexpected usage surge | Rate limiting + Azure spending alerts |

---

## 9. Team & Roles

| Role | Responsibility | FTE |
|------|---------------|-----|
| BI Developer | Dashboard, DAX measures, data model | 1 |
| Data Engineer | Knowledge extraction pipeline, embeddings | 1 |
| AI/ML Engineer | RAG chain, NL-to-DAX agent, LLM tuning | 1 |
| Full-Stack Developer | Chatbot UI, Teams integration | 0.5 |
| Project Manager | Coordination, stakeholder communication | 0.5 |
| IT/Infra | Azure provisioning, security, networking | 0.25 |

---

## 10. Success Metrics

| Metric | Target |
|--------|--------|
| Answer accuracy (judged by BI team) | ≥ 90% |
| User satisfaction (thumbs up rate) | ≥ 80% |
| Average response latency | < 5 seconds |
| Adoption (monthly active chatbot users) | 50+ within 3 months |
| Reduction in ad-hoc BI request tickets | 30% decrease |

---

## Appendix A — Folder Structure (Recommended Repo Layout)

```
maven-market-bi-assistant/
├── data/
│   ├── raw/                          # Original CSVs
│   └── cleaned/                      # Cleaned CSVs
├── powerbi/
│   ├── MavenMarket.pbix             # Power BI file
│   └── extracted/                    # pbi-tools output
├── knowledge/
│   ├── schema.json                  # Table & column metadata
│   ├── measures.json                # DAX measure definitions
│   ├── relationships.json           # Model relationships
│   ├── visual_metadata.json         # Dashboard visual definitions
│   └── insights.jsonl               # Pre-computed insight snippets
├── ai/
│   ├── embed_pipeline.py            # Embedding & vector store indexer
│   ├── rag_chain.py                 # RAG Q&A chain
│   ├── nl2dax_agent.py              # NL-to-DAX agent
│   └── config.py                    # Azure endpoints, keys (use env vars)
├── chatbot/
│   ├── app.py                       # Bot Framework / Streamlit app
│   ├── cards/                       # Adaptive Card templates
│   └── guardrails.py                # Content safety & PII filtering
├── tests/
│   ├── test_rag.py
│   └── test_nl2dax.py
├── .env.example
├── requirements.txt
└── README.md
```

---

*This framework is ready for corporate presentation and team kickoff. Each phase is independently deliverable and demoable.*
