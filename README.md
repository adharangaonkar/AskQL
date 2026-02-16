# AskQL

A natural language to SQL agent built with LangChain and LangGraph. Converts questions in plain English to SQL queries, executes them on a DuckDB database, and returns formatted results.

## Features

- **Query Generation**: Converts natural language to DuckDB SQL
- **Automatic Execution**: Runs queries and returns real data
- **Validation**: Enforces SELECT-only queries for safety
- **Error Correction**: Automatically fixes common SQL mistakes (up to 3 retries)
- **Multi-Node Workflow**: Uses LangGraph for robust query processing

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your OpenAI API key in `.env`:
```
OPENAI_API_KEY=your_key_here
```

3. Create the sample database:
```bash
python data/setup_database.py
```

## Usage

Run the agent with test queries:
```bash
python askQL.py
```

Or use it programmatically:
```python
from askQL import BasicSQLAgent
import os

agent = BasicSQLAgent(os.getenv("OPENAI_API_KEY"))
result = agent.query("How many customers are there?")

if result["success"]:
    print(result["results"])
```

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

## Architecture

The agent uses a 5-node LangGraph workflow with conditional routing:

```
┌─────────────────┐
│ User Question   │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Generate SQL   │  LLM creates SQL from question + schema
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Validate SQL   │  Check syntax & safety (SELECT-only)
└────────┬────────┘
         │
         ├──── invalid ────────────────┐
         │                              │
         │ valid                        v
         v                            [END]
┌─────────────────┐                 (return error)
│ Execute Query   │  Run SQL on DuckDB
└────────┬────────┘
         │
         ├──── error ──────┐
         │                 │
         │ success         v
         v            ┌─────────────────┐
┌─────────────────┐  │  Correct SQL    │  LLM fixes error
│ Format Results  │  └────────┬────────┘
└────────┬────────┘           │
         │                    │ retry (max 3)
         v                    │
       [END]                  │
   (return results)           │
         ^                    │
         │                    │
         └────────────────────┘
```

### Workflow Nodes

1. **Generate SQL**: LLM converts natural language to SQL using database schema
2. **Validate SQL**: Checks syntax with DuckDB EXPLAIN and enforces SELECT-only
3. **Execute Query**: Runs validated SQL and captures results
4. **Correct SQL**: Uses LLM to fix failed queries (triggered on execution errors)
5. **Format Results**: Converts raw data to readable tables

### Conditional Routing

- After **Validate**: `valid` → Execute | `invalid` → END
- After **Execute**: `success` → Format | `error` → Correct (if retries < 3) | `max_retries` → END

## Database Schema

The sample database includes:

- **customers** (50 rows): customer_id, name, email, age, city, signup_date
- **products** (30 rows): product_id, product_name, category, price, in_stock
- **orders** (200 rows): order_id, customer_id, product_id, quantity, order_date, total_amount

## Project Structure

```
AskQL/
├── askQL.py                 # Main agent implementation
├── data/
│   ├── setup_database.py    # Database creation script
│   └── askql.duckdb         # DuckDB database file
├── database_schema.csv      # Schema definition
├── test_correction.py       # Test script for error handling
├── requirements.txt         # Python dependencies
└── .env                     # API keys (not in git)
```

## Technical Details

- **Framework**: LangChain + LangGraph for agent orchestration
- **LLM**: OpenAI GPT-3.5-turbo
- **Database**: DuckDB (in-process, file-based)
- **Language**: Python 3.11+

## Testing

Run the test suite:
```bash
python askQL.py              # Run main test cases
python test_correction.py    # Test error handling
```

## License

MIT
