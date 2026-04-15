# 📊 MavenMarket BI Chatbot

AI-powered chatbot that answers business questions from a Power BI dashboard using RAG (Retrieval-Augmented Generation).

Ask natural language questions → get answers with charts, backed by 119 pre-computed insights + live CSV querying.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-1.56-red)
![LangChain](https://img.shields.io/badge/LangChain-0.3-green)
![GPT-4o](https://img.shields.io/badge/LLM-GPT--4o-purple)

---

## Features

- **Knowledge Base RAG** — 119 pre-computed insights embedded in FAISS vector store
- **Live CSV Fallback** — GPT-4o generates pandas code for questions not in the knowledge base
- **Auto Charts** — Plotly charts auto-generated from answer data (bar, pie, line, horizontal bar)
- **Chart Preferences** — Say "show me a pie chart" and it respects your choice
- **Conversation Memory** — Handles follow-up questions using chat history
- **Glassmorphism UI** — Modern dark theme with animated gradients and frosted glass effects
- **Tunable Parameters** — Sidebar sliders for temperature, max tokens, top-p, retrieval chunks

## Tech Stack

| Component | Technology |
|-----------|-----------|
| LLM | GPT-4o (via GitHub Models free tier) |
| Embeddings | text-embedding-3-small |
| Vector Store | FAISS (local) |
| Framework | LangChain |
| UI | Streamlit + Plotly |
| Data | 7 MavenMarket CSV tables (269K transactions) |

## Project Structure

```
cleaned/
├── data/
│   ├── csvs/                     # 7 MavenMarket CSV files
│   └── pbix/                     # Power BI file + extracted contents
│
└── chatbot_project/
    ├── ai/                       # Core engine
    │   ├── rag_chain.py          # RAG pipeline, prompt, retrieval
    │   ├── chart_builder.py      # Auto-chart detection from answers
    │   ├── pandas_query.py       # Live CSV query fallback
    │   ├── embed_pipeline.py     # FAISS embedding builder
    │   └── vectorstore/          # FAISS index (auto-generated)
    ├── app/
    │   └── app.py                # Streamlit UI
    ├── knowledge/
    │   ├── active/               # 4 files used by RAG pipeline
    │   └── raw/                  # Intermediate PBIX extraction files
    ├── scripts/                  # Knowledge base build scripts
    ├── tests/                    # Accuracy test suite
    ├── docs/                     # Framework documentation
    ├── .env.example              # Environment template
    └── requirements.txt          # Python dependencies
```

## Quick Start

### 1. Clone & Setup

```bash
git clone https://github.com/tejasvee0905/Bi_cahtbot.git
cd Bi_cahtbot/chatbot_project
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 2. Configure API Key

Get a free GitHub Models token from [github.com/settings/tokens](https://github.com/settings/tokens), then:

```bash
cp .env.example .env
# Edit .env and replace ghp_your_token_here with your actual token
```

### 3. Build Knowledge Base & Embeddings

```bash
python scripts/build_knowledge_v2.py
python -m ai.embed_pipeline
```

### 4. Run

```bash
streamlit run app/app.py
```

Open **http://localhost:8501** and start asking questions.

## Example Prompts

| Prompt | What You Get |
|--------|-------------|
| What is the total revenue? | Single KPI answer |
| Break down revenue by country | Table + bar chart |
| Customer gender split | Table + pie chart |
| Show me a pie chart of revenue by store type | Forced pie chart |
| Monthly revenue trend in 1998 | Table + line chart |
| Which store has the highest profit? | Ranked answer |
| What does Profit Margin measure? | DAX formula + explanation |

## How It Works

1. **Question** → Local keyword expansion generates alternative queries
2. **Retrieval** → FAISS similarity search + keyword fallback (no API needed if rate-limited)
3. **Answer** → GPT-4o generates response using retrieved context
4. **Fallback** → If knowledge base doesn't have the answer, GPT-4o writes pandas code to query CSVs directly
5. **Chart** → Answer is parsed for tables/lists → auto-generates Plotly chart with smart type selection

## API Limits (GitHub Models Free Tier)

- 150 requests/day (shared between LLM + embeddings)
- 15 requests/minute
- Built-in fallbacks for when limits are hit (keyword retrieval, cached embeddings)

---

Built with LangChain, FAISS, Streamlit, and GPT-4o.
