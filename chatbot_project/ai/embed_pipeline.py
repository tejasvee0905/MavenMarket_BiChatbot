import os
import json
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document

load_dotenv()

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
KNOWLEDGE_DIR = os.path.join(PROJECT_ROOT, "knowledge", "active")
VECTORSTORE_DIR = os.path.join(os.path.dirname(__file__), "vectorstore")

def load_schema() -> list[Document]:
    """Load schema.json as documents."""
    with open(os.path.join(KNOWLEDGE_DIR, "schema.json")) as f:
        schema = json.load(f)

    docs = []
    # Model overview
    docs.append(Document(
        page_content=f"The {schema['model_name']} data model is a star schema with {len(schema['tables'])} tables and {len(schema['relationships'])} relationships.",
        metadata={"source": "schema", "topic": "model_overview"}
    ))

    # Each table
    for tname, tinfo in schema["tables"].items():
        cols_desc = ""
        if "columns" in tinfo:
            cols_desc = "\n".join([f"  - {cname}: {cinfo['type']} — {cinfo['description']}" for cname, cinfo in tinfo["columns"].items()])

        content = f"Table: {tname}\nRole: {tinfo['role']}\nGrain: {tinfo.get('grain', 'N/A')}\nRow Count: {tinfo['row_count']}\nColumns:\n{cols_desc}"
        if "calculated_columns" in tinfo:
            content += "\nCalculated Columns:\n" + "\n".join([f"  - {k}: {v}" for k, v in tinfo["calculated_columns"].items()])
        if "description" in tinfo:
            content += f"\nDescription: {tinfo['description']}"

        docs.append(Document(
            page_content=content,
            metadata={"source": "schema", "topic": "table_definition", "table": tname}
        ))

    # Relationships
    for rel in schema["relationships"]:
        content = f"Relationship: {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']} ({rel['cardinality']}, active={rel['active']})"
        if "note" in rel:
            content += f"\nNote: {rel['note']}"
        docs.append(Document(
            page_content=content,
            metadata={"source": "schema", "topic": "relationship"}
        ))

    return docs


def load_measures() -> list[Document]:
    """Load measures.json as documents."""
    with open(os.path.join(KNOWLEDGE_DIR, "measures.json")) as f:
        measures = json.load(f)

    docs = []
    for m in measures:
        content = (
            f"DAX Measure: {m['name']}\n"
            f"Table: {m['table']}\n"
            f"Expression: {m['expression']}\n"
            f"Description: {m['description']}\n"
            f"Format: {m['format']}\n"
            f"Used in pages: {', '.join(m['used_in_pages'])}\n"
            f"Used in visuals: {', '.join(m['used_in_visuals'])}"
        )
        docs.append(Document(
            page_content=content,
            metadata={"source": "measures", "topic": "dax_measure", "measure_name": m["name"]}
        ))
    return docs


def load_dashboard() -> list[Document]:
    """Load dashboard_summary.json as documents."""
    with open(os.path.join(KNOWLEDGE_DIR, "dashboard_summary.json")) as f:
        dashboard = json.load(f)

    docs = []
    # Overview
    docs.append(Document(
        page_content=f"Dashboard: {dashboard['report_name']}, Theme: {dashboard['theme']}, {dashboard['total_pages']} pages, {dashboard['total_visuals']} total visuals.",
        metadata={"source": "dashboard", "topic": "dashboard_overview"}
    ))

    # Each page
    for page in dashboard["pages"]:
        visuals_desc = "\n".join([f"  - {v['type']}: {v['shows']}" for v in page["visuals"]])
        content = (
            f"Dashboard Page: {page['name']}\n"
            f"Purpose: {page['purpose']}\n"
            f"Visual Count: {page['visual_count']}\n"
            f"Slicers/Filters: {', '.join(page['slicers'])}\n"
            f"Visuals:\n{visuals_desc}"
        )
        docs.append(Document(
            page_content=content,
            metadata={"source": "dashboard", "topic": "dashboard_page", "page": page["name"]}
        ))

    return docs


def load_insights() -> list[Document]:
    """Load insights.jsonl as documents."""
    docs = []
    with open(os.path.join(KNOWLEDGE_DIR, "insights.jsonl")) as f:
        for line in f:
            ins = json.loads(line.strip())
            content = f"Insight — {ins['topic']}:\n{ins['insight']}"
            docs.append(Document(
                page_content=content,
                metadata={"source": "insights", "topic": ins["topic"]}
            ))
    return docs


def build_vectorstore():
    """Load all knowledge, chunk, embed, and save FAISS index."""
    print("Loading knowledge documents...")
    all_docs = []
    all_docs.extend(load_schema())
    all_docs.extend(load_measures())
    all_docs.extend(load_dashboard())
    all_docs.extend(load_insights())
    print(f"  Loaded {len(all_docs)} documents")

    # Split into chunks — keep insights and measures whole, only split large schema docs
    splitter = RecursiveCharacterTextSplitter(chunk_size=1200, chunk_overlap=200)

    # Separate docs that should stay whole (insights, measures) from those that can be split
    keep_whole = [d for d in all_docs if d.metadata.get("source") in ("insights", "measures")]
    can_split = [d for d in all_docs if d.metadata.get("source") not in ("insights", "measures")]

    chunks = keep_whole + splitter.split_documents(can_split)
    print(f"  Split into {len(chunks)} chunks ({len(keep_whole)} kept whole)")

    # Embed using GitHub Models (free)
    print("Creating embeddings via GitHub Models...")
    embeddings = OpenAIEmbeddings(
        model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"),
        base_url=os.getenv("BASE_URL", "https://models.inference.ai.azure.com"),
        api_key=os.getenv("GITHUB_TOKEN"),
    )

    # Build FAISS index
    print("Building FAISS vector store...")
    vectorstore = FAISS.from_documents(chunks, embeddings)

    # Save locally
    os.makedirs(VECTORSTORE_DIR, exist_ok=True)
    vectorstore.save_local(VECTORSTORE_DIR)
    print(f"  Saved vector store to {VECTORSTORE_DIR}")
    print(f"\nDone! {len(chunks)} chunks indexed.")
    return vectorstore


if __name__ == "__main__":
    build_vectorstore()
