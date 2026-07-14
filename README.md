# AskQL

A natural language to SQL agent built with LangChain and LangGraph. Converts questions in plain English to SQL queries, executes them on a DuckDB database, and returns formatted results.

## Features

- **Query Generation**: Converts natural language to DuckDB SQL, including complex queries (joins, aggregations, window functions, subqueries)
- **Automatic Execution**: Runs queries and returns real data
- **Validation**: Enforces SELECT-only queries for safety
- **Error Correction**: Automatically fixes both SQL syntax errors and execution errors (up to 3 retries) — safety rejections (non-SELECT queries) are never retried
- **Multi-Node Workflow**: Uses LangGraph for robust query processing

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file in the project root (it's gitignored, so it's safe to put secrets in it). AskQL supports three LLM providers, selected via `LLM_PROVIDER`. Set only the block for the provider you're using:

```dotenv
# Which LLM backend to use: openai | ollama | lmstudio
LLM_PROVIDER=openai

# --- OpenAI (only needed if LLM_PROVIDER=openai) ---
OPENAI_API_KEY=your_key_here

# --- Ollama (only needed if LLM_PROVIDER=ollama) ---
# OLLAMA_BASE_URL=http://localhost:11434
# OLLAMA_MODEL=llama3.1

# --- LM Studio (only needed if LLM_PROVIDER=lmstudio) ---
# LMSTUDIO_BASE_URL=http://localhost:1234/v1
# LMSTUDIO_MODEL=local-model
```

**OpenAI** — just set `OPENAI_API_KEY`. `LLM_PROVIDER=openai` is the default if unset.

**Ollama** (run models locally with [Ollama](https://ollama.com)):
- Install Ollama and pull a model, e.g. `ollama pull qwen2.5-coder:14b` (a code-tuned model like `qwen2.5-coder` tends to generate cleaner SQL than general chat models like `llama3.1`)
- Set `LLM_PROVIDER=ollama` and `OLLAMA_MODEL` to the model you pulled
- `OLLAMA_BASE_URL` only needs to be set if Ollama isn't running on the default `http://localhost:11434`

**LM Studio** (run models locally with [LM Studio](https://lmstudio.ai)):
- Start LM Studio's local server (Developer tab → Start Server) with a model loaded
- Set `LLM_PROVIDER=lmstudio` and `LMSTUDIO_MODEL` to match the loaded model's name
- `LMSTUDIO_BASE_URL` only needs to be set if it isn't running on the default `http://localhost:1234/v1`

3. Create the sample database:
```bash
python data/setup_database.py
```

## Usage

Run the bundled example queries:
```bash
python demo_queries.py
```

`askQL.py` is the library entry point; running it directly only prints a short
usage message. Import `BasicSQLAgent` or `query` to use AskQL in your own code.

Or use it programmatically:
```python
from askQL import BasicSQLAgent
import os

# provider defaults to LLM_PROVIDER from .env if not passed explicitly
agent = BasicSQLAgent(openai_api_key=os.getenv("OPENAI_API_KEY"), provider="openai")
result = agent.query("How many customers are there?")

if result["success"]:
    print(result["results"])
```

### Demo Notebooks

The [demo/](demo/) folder has a runnable notebook per provider — `AskQL_Demo_OpenAI.ipynb`, `AskQL_Demo_Ollama.ipynb`, `AskQL_Demo_LMStudio.ipynb` — each walking through the same set of example queries (counts, joins, visualization, validation failures, CSV export, etc.) against that provider. They read `.env` from the project root automatically.

## Examples

**Simple Query:**
```
Question: "How many customers are there?"
SQL: SELECT COUNT(customer_id) AS total_customers FROM customers;
Results:
 total_customers
              50
(1 rows returned)
```

**Complex Query with Joins:**
```
Question: "Show me the top 5 customers by total spending"
SQL: SELECT c.name, SUM(o.total_amount) AS total_spent
     FROM customers c
     JOIN orders o ON c.customer_id = o.customer_id
     GROUP BY c.customer_id, c.name
     ORDER BY total_spent DESC
     LIMIT 5;
Results:
       name total_spent
Customer 47    32179.21
Customer 18    28370.41
Customer 41    26227.07
Customer 24    25947.68
Customer 46    24025.98
(5 rows returned)
```

**Validation (Safety):**
```
Question: "Delete all customers"
SQL: DELETE FROM customers;
Error: Only SELECT queries are allowed for safety
```

**Complex Query with Window Functions (self-corrected):**
```
Question: "For each city, find the single customer with the highest total
           spending, ranking customers by spending within each city"
```
The model is instructed to express this kind of "rank, then filter" logic as a
subquery/derived table rather than a `WITH` (CTE) clause. If its first attempt
puts the window function directly in `WHERE`/`HAVING` (invalid SQL), validation
catches the syntax error and routes it back through **Correct SQL** to retry —
same retry budget as execution errors, capped at 3 attempts:
```sql
SELECT city, name, customer_id, total_spending, rank
FROM (
    SELECT c.city, c.name, c.customer_id, SUM(o.total_amount) AS total_spending,
           RANK() OVER (PARTITION BY c.city ORDER BY SUM(o.total_amount) DESC) AS rank
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.city, c.name, c.customer_id
) subquery
WHERE rank = 1;
```

## Architecture

The agent uses a 6-node LangGraph workflow with conditional routing:

```
┌─────────────────┐
│ User Question   │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Get Schema     │  Introspects the live DuckDB database's schema
└────────┬────────┘
         │
         ├──── error ──────────────────┐
         │                              │
         │ ok                           v
         v                            [END]
┌─────────────────┐                 (return error)
│  Generate SQL   │  LLM creates SQL from question + schema
└────────┬────────┘
         │
         v
┌─────────────────┐  <───────────────────┐
│  Validate SQL   │  Check syntax (EXPLAIN)│
└────────┬────────┘  & safety (SELECT-only)│
         │                                 │
         ├── safety-blocked ──────────────────┐
         │   (never retried)                  │
         ├── syntax error, retries < 3 ──┐     │
         │                                v     │
         │ valid                  ┌─────────────────┐
         v                        │  Correct SQL    │  LLM fixes the error
┌─────────────────┐               └────────┬────────┘
│ Execute Query   │  Run SQL on DuckDB     │
└────────┬────────┘                        │
         │                                 │
         ├── error, retries < 3 ───────────┘
         │
         │ success                       [END]
         v                           (return error /
┌─────────────────┐                  max retries reached)
│ Format Results  │
└────────┬────────┘
         │
         v
       [END]
   (return results)
```

### Workflow Nodes

1. **Get Schema**: Introspects the live DuckDB database's `information_schema` to build the schema description used by the LLM (no static schema file needed)
2. **Generate SQL**: LLM converts natural language to SQL using the introspected schema. Instructed to use subqueries/derived tables instead of `WITH` (CTE) clauses for complex logic like rankings or filtering on an aggregate
3. **Validate SQL**: Checks syntax with DuckDB EXPLAIN and enforces SELECT-only
4. **Execute Query**: Runs validated SQL and captures results
5. **Correct SQL**: Uses LLM to fix failed queries — triggered by *either* a validation syntax error or an execution error (but never by a SELECT-only safety rejection, which always fails immediately)
6. **Format Results**: Converts raw data to readable tables

### Conditional Routing

- After **Get Schema**: `ok` → Generate SQL | `error` → END
- After **Validate**: `valid` → Execute | syntax error & retries < 3 → Correct SQL | safety-blocked or max retries → END
- After **Execute**: `success` → Format | `error` & retries < 3 → Correct SQL | `max_retries` → END
- After **Correct SQL**: always back to Validate SQL (re-checked before executing again)

## Database Schema

The sample database includes:

- **customers** (50 rows): customer_id, name, email, age, city, signup_date
- **products** (30 rows): product_id, product_name, category, price, in_stock
- **orders** (200 rows): order_id, customer_id, product_id, quantity, order_date, total_amount

## Project Structure

```
AskQL/
├── askQL.py                 # Main agent implementation
├── demo/                    # Runnable demo notebooks, one per provider
│   ├── AskQL_Demo_OpenAI.ipynb
│   ├── AskQL_Demo_Ollama.ipynb
│   └── AskQL_Demo_LMStudio.ipynb
├── data/
│   ├── setup_database.py    # Database creation script
│   ├── askql.duckdb         # DuckDB database file (gitignored, generated)
│   ├── schema.sql           # DDL used to create the sample database (agent introspects the live DB instead)
│   └── duckdb.ipynb         # DuckDB exploration notebook
├── demo_queries.py          # Script running a handful of sample queries
├── test_correction.py       # Test script for error handling
├── requirements.txt         # Python dependencies
└── .env                     # LLM_PROVIDER + provider-specific config (gitignored, not in git)
```

## Technical Details

- **Framework**: LangChain + LangGraph for agent orchestration
- **LLM**: OpenAI GPT-3.5-turbo (default), or local models via Ollama or LM Studio
- **Database**: DuckDB (in-process, file-based)
- **Language**: Python 3.11+

## Testing

Run the example queries and error-handling checks:
```bash
python demo_queries.py       # Run sample natural-language queries
python test_correction.py    # Test error handling
```

## License

MIT
