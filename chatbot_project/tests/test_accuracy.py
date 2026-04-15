"""
Accuracy Test Suite for MavenMarket BI Chatbot
Tests RAG pipeline against ground-truth data from the knowledge base.
"""
import os, sys, json, re, time

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()

from ai.rag_chain import get_rag_chain, ask_with_history

# ═══════════════════════════════════════════════════════════════
# TEST CASES: (question, expected_keywords, category)
#   - expected_keywords: list of strings that MUST appear in answer
#   - we check if the answer contains the key facts
# ═══════════════════════════════════════════════════════════════

TEST_CASES = [
    # ── Category: KPI Retrieval (exact numbers) ──
    {
        "question": "What is the total revenue?",
        "expected_keywords": ["1,764,546", "revenue"],
        "category": "KPI Retrieval"
    },
    {
        "question": "What is the total profit?",
        "expected_keywords": ["1,052,818", "profit"],
        "category": "KPI Retrieval"
    },
    {
        "question": "What is the profit margin?",
        "expected_keywords": ["59.7", "margin"],
        "category": "KPI Retrieval"
    },
    {
        "question": "How many transactions are there in total?",
        "expected_keywords": ["269,720"],
        "category": "KPI Retrieval"
    },
    {
        "question": "What is the return rate?",
        "expected_keywords": ["1.0", "return"],
        "category": "KPI Retrieval"
    },
    {
        "question": "How many customers does MavenMarket have?",
        "expected_keywords": ["8,842"],
        "category": "KPI Retrieval"
    },
    {
        "question": "What is the total cost?",
        "expected_keywords": ["711,727", "cost"],
        "category": "KPI Retrieval"
    },

    # ── Category: Country/Region Analysis ──
    {
        "question": "Which country has the highest revenue?",
        "expected_keywords": ["USA", "1,177,956"],
        "category": "Country/Region"
    },
    {
        "question": "What is the revenue for Mexico?",
        "expected_keywords": ["478,915", "Mexico"],
        "category": "Country/Region"
    },
    {
        "question": "What is the revenue for Canada?",
        "expected_keywords": ["107,674", "Canada"],
        "category": "Country/Region"
    },
    {
        "question": "Which sales region generates the most revenue?",
        "expected_keywords": ["North West", "847,826"],
        "category": "Country/Region"
    },

    # ── Category: Store Analysis ──
    {
        "question": "Which store has the highest revenue?",
        "expected_keywords": ["Store 13", "170,398"],
        "category": "Store Analysis"
    },
    {
        "question": "Which store has the highest return rate?",
        "expected_keywords": ["Store 8", "1.2"],
        "category": "Store Analysis"
    },
    {
        "question": "What is the revenue by store type?",
        "expected_keywords": ["Supermarket", "789,601"],
        "category": "Store Analysis"
    },
    {
        "question": "What is the weekend revenue?",
        "expected_keywords": ["502,095", "28.5"],
        "category": "Store Analysis"
    },

    # ── Category: Product Analysis ──
    {
        "question": "What is the top product by revenue?",
        "expected_keywords": ["Hermanos Green Pepper", "2,489"],
        "category": "Product Analysis"
    },
    {
        "question": "What is the most returned product?",
        "expected_keywords": ["Hermanos Red Pepper", "17"],
        "category": "Product Analysis"
    },
    {
        "question": "Which brand has the highest revenue?",
        "expected_keywords": ["Hermanos", "56,659"],
        "category": "Product Analysis"
    },
    {
        "question": "What product has the lowest profit?",
        "expected_keywords": ["Denny Glass Cleaner", "128"],
        "category": "Product Analysis"
    },

    # ── Category: Customer Analysis ──
    {
        "question": "What is the revenue split by gender?",
        "expected_keywords": ["Female", "891,726", "Male", "872,819"],
        "category": "Customer Analysis"
    },
    {
        "question": "Which income bracket spends the most?",
        "expected_keywords": ["$30K - $50K", "580,785"],
        "category": "Customer Analysis"
    },
    {
        "question": "What is the average revenue per customer?",
        "expected_keywords": ["199.56"],
        "category": "Customer Analysis"
    },
    {
        "question": "Which member card tier generates the most revenue?",
        "expected_keywords": ["Bronze", "986,502"],
        "category": "Customer Analysis"
    },
    {
        "question": "Revenue by marital status?",
        "expected_keywords": ["Married", "875,173", "Single", "889,373"],
        "category": "Customer Analysis"
    },

    # ── Category: Time Analysis ──
    {
        "question": "What was the revenue in 1997?",
        "expected_keywords": ["565,238", "1997"],
        "category": "Time Analysis"
    },
    {
        "question": "What was the year-over-year revenue growth?",
        "expected_keywords": ["112.2", "growth"],
        "category": "Time Analysis"
    },
    {
        "question": "Which was the best revenue month?",
        "expected_keywords": ["December 1998", "120,160"],
        "category": "Time Analysis"
    },
    {
        "question": "What is the quarterly revenue for 1998 Q4?",
        "expected_keywords": ["326,384"],
        "category": "Time Analysis"
    },

    # ── Category: Dashboard Awareness ──
    {
        "question": "How many pages does the dashboard have?",
        "expected_keywords": ["3", "page"],
        "category": "Dashboard Awareness"
    },
    {
        "question": "What does page 2 of the dashboard show?",
        "expected_keywords": ["Store", "Performance"],
        "category": "Dashboard Awareness"
    },

    # ── Category: DAX Measure Knowledge ──
    {
        "question": "What is the DAX formula for Total Revenue?",
        "expected_keywords": ["SUMX", "quantity", "product_retail_price"],
        "category": "DAX Knowledge"
    },
    {
        "question": "How is Profit Margin calculated in DAX?",
        "expected_keywords": ["DIVIDE", "Total Profit", "Total Revenue"],
        "category": "DAX Knowledge"
    },

    # ── Category: Hallucination Check (should refuse gracefully) ──
    {
        "question": "What is the CEO's name?",
        "expected_keywords": ["~don't have|not available|not include|no information|not contain|cannot find|don't know|outside"],
        "category": "Hallucination Guard"
    },
    {
        "question": "What is the stock price of MavenMarket?",
        "expected_keywords": ["~don't have|not available|not include|no information|not contain|cannot find|don't know|outside"],
        "category": "Hallucination Guard"
    },

    # ── Category: Analytical / Reasoning Questions ──
    {
        "question": "Why did revenue grow from 1997 to 1998?",
        "expected_keywords": ["112", "1997", "1998"],
        "category": "Analytical"
    },
    {
        "question": "Which country contributes the least revenue and what percentage is it?",
        "expected_keywords": ["Canada", "107,674"],
        "category": "Analytical"
    },
    {
        "question": "Compare weekend vs weekday revenue",
        "expected_keywords": ["502,095", "1,262,450"],
        "category": "Analytical"
    },
    {
        "question": "What are the KPIs shown on the executive page?",
        "expected_keywords": ["Total Profit", "Total Revenue", "Profit Margin"],
        "category": "Analytical"
    },
]

# Follow-up conversation test pairs
FOLLOWUP_TESTS = [
    {
        "conversation": [
            {"question": "What is the revenue by country?", "expected_keywords": ["USA", "Mexico", "Canada"]},
            {"question": "What about just Canada?", "expected_keywords": ["Canada", "107,674"]},
        ],
        "category": "Conversation Follow-up"
    },
    {
        "conversation": [
            {"question": "Which store has the highest revenue?", "expected_keywords": ["Store 13"]},
            {"question": "And what is its return rate?", "expected_keywords": ["Store 13", "0.9"]},
        ],
        "category": "Conversation Follow-up"
    },
]


def check_answer(answer: str, expected_keywords: list[str]) -> tuple[bool, list[str]]:
    """Check if answer contains all expected keywords (case-insensitive).
    Keywords starting with ~ are treated as regex alternation patterns."""
    answer_lower = answer.lower()
    missing = []
    for kw in expected_keywords:
        if kw.startswith("~"):
            # Regex alternation: ~word1|word2|word3 — must match at least one
            pattern = kw[1:]
            if not re.search(pattern, answer_lower):
                missing.append(kw)
        else:
            if kw.lower() not in answer_lower:
                missing.append(kw)
    return len(missing) == 0, missing


def run_tests():
    print("=" * 70)
    print("  MAVENMARKET BI CHATBOT — ACCURACY TEST SUITE v2")
    print("  (GPT-4o + Multi-Query Retrieval + Conversation Memory)")
    print("=" * 70)
    print(f"\n  Standalone tests: {len(TEST_CASES)}")
    print(f"  Follow-up conversations: {len(FOLLOWUP_TESTS)}")
    print(f"  Loading RAG chain...\n")

    components = get_rag_chain(temperature=0.0, max_tokens=1024, top_p=1.0, top_k=12)

    results = []
    categories = {}

    # ── Standalone tests ──
    for i, tc in enumerate(TEST_CASES):
        q = tc["question"]
        expected = tc["expected_keywords"]
        cat = tc["category"]

        print(f"  [{i+1:02d}/{len(TEST_CASES)}] {cat}: {q}")

        start = time.time()
        try:
            result = ask_with_history(components, q)
            answer = result["answer"]
            elapsed = time.time() - start

            passed, missing = check_answer(answer, expected)
            sources = result["sources"]

            status = "PASS" if passed else "FAIL"
            print(f"         {status} ({elapsed:.1f}s) | Sources: {[s.split(':')[0] for s in sources[:4]]}")
            if not passed:
                print(f"         Missing: {missing}")
                print(f"         Answer: {answer[:150]}...")

            results.append({
                "question": q, "category": cat, "passed": passed,
                "missing": missing, "answer": answer,
                "sources": sources, "latency_s": round(elapsed, 2)
            })

            if cat not in categories:
                categories[cat] = {"pass": 0, "fail": 0, "latencies": []}
            categories[cat]["pass" if passed else "fail"] += 1
            categories[cat]["latencies"].append(elapsed)

        except Exception as e:
            elapsed = time.time() - start
            print(f"         ERROR ({elapsed:.1f}s): {e}")
            results.append({
                "question": q, "category": cat, "passed": False,
                "missing": expected, "answer": f"ERROR: {e}",
                "sources": [], "latency_s": round(elapsed, 2)
            })
            if cat not in categories:
                categories[cat] = {"pass": 0, "fail": 0, "latencies": []}
            categories[cat]["fail"] += 1
            categories[cat]["latencies"].append(elapsed)

        time.sleep(2)  # Rate limit: 15 req/min (1 LLM call + 1 embed call per question)

    # ── Follow-up conversation tests ──
    print(f"\n  --- CONVERSATION FOLLOW-UP TESTS ---\n")
    for ci, conv_test in enumerate(FOLLOWUP_TESTS):
        cat = conv_test["category"]
        chat_history = []

        for ti, turn in enumerate(conv_test["conversation"]):
            q = turn["question"]
            expected = turn["expected_keywords"]
            label = f"Conv {ci+1}, Turn {ti+1}"

            print(f"  [{label}] {q}")

            start = time.time()
            try:
                result = ask_with_history(components, q, chat_history=chat_history)
                answer = result["answer"]
                elapsed = time.time() - start

                passed, missing = check_answer(answer, expected)
                sources = result["sources"]

                status = "PASS" if passed else "FAIL"
                print(f"         {status} ({elapsed:.1f}s)")
                if not passed:
                    print(f"         Missing: {missing}")
                    print(f"         Answer: {answer[:150]}...")

                chat_history.append((q, answer))

                results.append({
                    "question": f"[{label}] {q}", "category": cat, "passed": passed,
                    "missing": missing, "answer": answer,
                    "sources": sources, "latency_s": round(elapsed, 2)
                })

                if cat not in categories:
                    categories[cat] = {"pass": 0, "fail": 0, "latencies": []}
                categories[cat]["pass" if passed else "fail"] += 1
                categories[cat]["latencies"].append(elapsed)

            except Exception as e:
                elapsed = time.time() - start
                print(f"         ERROR ({elapsed:.1f}s): {e}")
                chat_history.append((q, f"Error: {e}"))
                results.append({
                    "question": f"[{label}] {q}", "category": cat, "passed": False,
                    "missing": expected, "answer": f"ERROR: {e}",
                    "sources": [], "latency_s": round(elapsed, 2)
                })
                if cat not in categories:
                    categories[cat] = {"pass": 0, "fail": 0, "latencies": []}
                categories[cat]["fail"] += 1
                categories[cat]["latencies"].append(elapsed)

            time.sleep(2)

    # ═══════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════
    total_pass = sum(1 for r in results if r["passed"])
    total_fail = sum(1 for r in results if not r["passed"])
    total = len(results)
    accuracy = total_pass / total * 100 if total > 0 else 0
    avg_latency = sum(r["latency_s"] for r in results) / total if total > 0 else 0

    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)
    print(f"\n  Overall Accuracy:  {total_pass}/{total} ({accuracy:.1f}%)")
    print(f"  Average Latency:   {avg_latency:.2f}s per query")
    print(f"  Pass: {total_pass}  |  Fail: {total_fail}")

    print(f"\n  {'Category':<25} {'Pass':>5} {'Fail':>5} {'Accuracy':>10} {'Avg Latency':>12}")
    print("  " + "-" * 60)
    for cat, stats in sorted(categories.items()):
        cat_total = stats["pass"] + stats["fail"]
        cat_acc = stats["pass"] / cat_total * 100
        cat_lat = sum(stats["latencies"]) / len(stats["latencies"])
        print(f"  {cat:<25} {stats['pass']:>5} {stats['fail']:>5} {cat_acc:>9.1f}% {cat_lat:>10.2f}s")

    # Failed tests detail
    failed = [r for r in results if not r["passed"]]
    if failed:
        print(f"\n  FAILED TESTS ({len(failed)}):")
        for r in failed:
            print(f"    - [{r['category']}] {r['question']}")
            print(f"      Missing keywords: {r['missing']}")
            print(f"      Answer snippet: {r['answer'][:120]}...")

    # Save results to JSON
    output_path = os.path.join(os.path.dirname(__file__), "test_results.json")
    with open(output_path, "w") as f:
        json.dump({
            "summary": {
                "total": total,
                "passed": total_pass,
                "failed": total_fail,
                "accuracy_pct": round(accuracy, 1),
                "avg_latency_s": round(avg_latency, 2),
            },
            "by_category": {cat: {"pass": s["pass"], "fail": s["fail"],
                                   "accuracy_pct": round(s["pass"]/(s["pass"]+s["fail"])*100, 1)}
                            for cat, s in categories.items()},
            "results": results
        }, f, indent=2)
    print(f"\n  Full results saved to: {output_path}")
    print("=" * 70)


if __name__ == "__main__":
    run_tests()
