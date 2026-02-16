import os
import time
from typing import Any, Dict

import duckdb
import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph

load_dotenv()

MAX_RETRIES = 3
DEFAULT_MODEL = "gpt-3.5-turbo"


def clean_sql(sql_text: str) -> str:
    """Remove markdown code fences if present."""
    sql = (sql_text or "").strip()
    if sql.startswith("```sql"):
        return sql.replace("```sql", "").replace("```", "").strip()
    if sql.startswith("```"):
        return sql.replace("```", "").strip()
    return sql


def load_schema_text(schema_csv_path: str) -> str:
    """Load schema CSV and render it as prompt-friendly text."""
    schema_df = pd.read_csv(schema_csv_path)

    parts = []
    for table in schema_df["table_name"].unique():
        parts.append(f"\nTable: {table}")
        parts.append("Columns:")
        table_cols = schema_df[schema_df["table_name"] == table]
        for _, col in table_cols.iterrows():
            parts.append(f"  - {col['column_name']} ({col['data_type']})")

    return "\n".join(parts)


def make_generate_sql_node(llm: ChatOpenAI, schema_info: str):
    def generate_sql(state: dict) -> dict:
        try:
            prompt = f"""You are a SQL expert. Generate a DuckDB SQL query based on the user's question.

Database Schema:
{schema_info}

Rules:
1. Only use tables and columns from the schema
2. Use DuckDB SQL syntax
3. Return ONLY SQL (no explanation)
4. Ensure SQL is valid

User Question: {state['user_question']}

SQL Query:"""
            response = llm.invoke([SystemMessage(content=prompt)])
            state["generated_sql"] = clean_sql(str(response.content))
        except Exception as exc:
            state["error"] = f"SQL generation failed: {exc}"
        return state

    return generate_sql


def make_validate_sql_node(database_path: str):
    def validate_sql(state: dict) -> dict:
        sql = (state.get("generated_sql") or "").strip()
        if not sql:
            state["validation_error"] = "No SQL generated"
            return state

        if not sql.upper().startswith("SELECT"):
            state["validation_error"] = "Only SELECT queries are allowed for safety"
            return state

        try:
            conn = duckdb.connect(database_path, read_only=True)
            try:
                conn.execute(f"EXPLAIN {sql}")
                state["is_valid"] = True
            except Exception as exc:
                state["validation_error"] = f"SQL syntax error: {exc}"
            finally:
                conn.close()
        except Exception as exc:
            state["validation_error"] = f"Validation failed: {exc}"

        return state

    return validate_sql


def make_execute_query_node(database_path: str):
    def execute_query(state: dict) -> dict:
        sql = state.get("generated_sql", "")
        try:
            conn = duckdb.connect(database_path, read_only=True)
            try:
                start = time.time()
                result = conn.execute(sql).fetchall()
                columns = [desc[0] for desc in conn.description]
                state["execution_time"] = time.time() - start
                state["raw_results"] = [dict(zip(columns, row)) for row in result]
                state["rows_affected"] = len(result)
                state["execution_error"] = ""
            except Exception as exc:
                state["execution_error"] = str(exc)
            finally:
                conn.close()
        except Exception as exc:
            state["execution_error"] = f"Database connection failed: {exc}"

        return state

    return execute_query


def make_correct_sql_node(llm: ChatOpenAI, schema_info: str):
    def correct_sql(state: dict) -> dict:
        try:
            state["retry_count"] = state.get("retry_count", 0) + 1

            prompt = f"""The SQL query failed with this error:
{state['execution_error']}

Failed SQL:
{state['generated_sql']}

Original question: {state['user_question']}

Database Schema:
{schema_info}

This is attempt {state['retry_count']} of {MAX_RETRIES}.

Return a corrected DuckDB SQL query.
Rules:
1. Use only schema tables/columns
2. Use valid DuckDB SQL
3. Return ONLY SQL
4. Must be SELECT

Corrected SQL Query:"""

            response = llm.invoke([SystemMessage(content=prompt)])
            corrected_sql = clean_sql(str(response.content))

            history = state.setdefault("correction_history", [])
            history.append(
                {
                    "attempt": state["retry_count"],
                    "error": state["execution_error"],
                    "original_sql": state["generated_sql"],
                    "corrected_sql": corrected_sql,
                }
            )

            state["generated_sql"] = corrected_sql
            state["execution_error"] = ""
        except Exception as exc:
            state["error"] = f"SQL correction failed: {exc}"

        return state

    return correct_sql


def format_results(state: dict) -> dict:
    raw_results = state.get("raw_results", [])
    if not raw_results:
        state["formatted_results"] = "No results found."
        return state

    df = pd.DataFrame(raw_results)
    total_rows = len(df)
    preview = df.head(5).to_string(index=False)

    if total_rows > 5:
        preview += f"\n\n(Showing first 5 of {total_rows} rows)"
    else:
        preview += f"\n\n({total_rows} rows returned)"

    state["formatted_results"] = preview
    return state


def route_after_validation(state: dict) -> str:
    return "invalid" if state.get("validation_error") else "valid"


def route_after_execution(state: dict) -> str:
    if state.get("execution_error"):
        if state.get("retry_count", 0) < MAX_RETRIES:
            return "retry"
        return "max_retries"
    return "success"


def build_workflow(llm: ChatOpenAI, schema_info: str, database_path: str):
    workflow = StateGraph(dict)

    workflow.add_node("generate_sql", make_generate_sql_node(llm, schema_info))
    workflow.add_node("validate_sql", make_validate_sql_node(database_path))
    workflow.add_node("execute_query", make_execute_query_node(database_path))
    workflow.add_node("correct_sql", make_correct_sql_node(llm, schema_info))
    workflow.add_node("format_results", format_results)

    workflow.set_entry_point("generate_sql")
    workflow.add_edge("generate_sql", "validate_sql")

    workflow.add_conditional_edges(
        "validate_sql",
        route_after_validation,
        {"valid": "execute_query", "invalid": END},
    )

    workflow.add_conditional_edges(
        "execute_query",
        route_after_execution,
        {"success": "format_results", "retry": "correct_sql", "max_retries": END},
    )

    workflow.add_edge("correct_sql", "execute_query")
    workflow.add_edge("format_results", END)

    return workflow.compile()


def initial_state(question: str) -> dict:
    return {
        "user_question": question,
        "generated_sql": "",
        "error": "",
        "is_valid": False,
        "validation_error": "",
        "execution_error": "",
        "execution_time": 0.0,
        "rows_affected": 0,
        "raw_results": [],
        "formatted_results": "",
        "retry_count": 0,
        "correction_history": [],
    }


def build_result(question: str, final_state: dict) -> Dict[str, Any]:
    return {
        "question": question,
        "sql": final_state.get("generated_sql", ""),
        "results": final_state.get("formatted_results", ""),
        "raw_results": final_state.get("raw_results", []),
        "rows": final_state.get("rows_affected", 0),
        "execution_time": final_state.get("execution_time", 0.0),
        "validation_error": final_state.get("validation_error", ""),
        "execution_error": final_state.get("execution_error", ""),
        "error": final_state.get("error", ""),
        "retry_count": final_state.get("retry_count", 0),
        "success": not any(
            [
                final_state.get("validation_error"),
                final_state.get("execution_error"),
                final_state.get("error"),
            ]
        ),
    }


def create_query_runner(
    openai_api_key: str,
    schema_csv_path: str = "database_schema.csv",
    database_path: str = "data/askql.duckdb",
    model: str = DEFAULT_MODEL,
):
    llm = ChatOpenAI(api_key=openai_api_key, model=model, temperature=0)
    schema_info = load_schema_text(schema_csv_path)
    workflow = build_workflow(llm, schema_info, database_path)

    def run(question: str) -> Dict[str, Any]:
        final_state = workflow.invoke(initial_state(question))
        return build_result(question, final_state)

    return run, workflow


def query(
    question: str,
    openai_api_key: str,
    schema_csv_path: str = "database_schema.csv",
    database_path: str = "data/askql.duckdb",
    model: str = DEFAULT_MODEL,
) -> Dict[str, Any]:
    run, _ = create_query_runner(
        openai_api_key=openai_api_key,
        schema_csv_path=schema_csv_path,
        database_path=database_path,
        model=model,
    )
    return run(question)


class BasicSQLAgent:
    """Thin compatibility wrapper around function-first query pipeline."""

    def __init__(
        self,
        openai_api_key: str,
        schema_csv_path: str = "database_schema.csv",
        database_path: str = "data/askql.duckdb",
        model: str = DEFAULT_MODEL,
    ):
        self.run_query, self.workflow = create_query_runner(
            openai_api_key=openai_api_key,
            schema_csv_path=schema_csv_path,
            database_path=database_path,
            model=model,
        )

    def query(self, question: str) -> Dict[str, Any]:
        return self.run_query(question)


def main() -> None:
    print(
        "askQL.py is a library module. "
        "Run demo_queries.py for sample queries or import BasicSQLAgent/query in your code."
    )


if __name__ == "__main__":
    main()
