import os
import time
from typing import Any, Dict

import duckdb
import pandas as pd
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI
from langchain_ollama import ChatOllama
from langgraph.graph import END, StateGraph

load_dotenv()

MAX_RETRIES = 3
DEFAULT_MODELS = {
    "openai": "gpt-3.5-turbo",
    "ollama": "llama3.1",
    "lmstudio": "local-model",
}


def build_llm(provider: str = None, model: str = None, **kwargs):
    """Construct a chat LLM for the given provider (openai | ollama | lmstudio)."""
    provider = (provider or os.getenv("LLM_PROVIDER", "openai")).lower()

    if provider == "openai":
        api_key = kwargs.pop("api_key", None) or os.getenv("OPENAI_API_KEY")
        return ChatOpenAI(
            api_key=api_key,
            model=model or DEFAULT_MODELS["openai"],
            temperature=0,
            **kwargs,
        )

    if provider == "ollama":
        kwargs.pop("api_key", None)
        base_url = kwargs.pop("base_url", None) or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        # The pinned langchain-ollama version has no base_url constructor param;
        # its underlying ollama.Client reads OLLAMA_HOST from the environment instead.
        os.environ["OLLAMA_HOST"] = base_url
        return ChatOllama(
            model=model or os.getenv("OLLAMA_MODEL", DEFAULT_MODELS["ollama"]),
            temperature=0,
            **kwargs,
        )

    if provider == "lmstudio":
        kwargs.pop("api_key", None)
        base_url = kwargs.pop("base_url", None) or os.getenv("LMSTUDIO_BASE_URL", "http://localhost:1234/v1")
        return ChatOpenAI(
            api_key="lm-studio",  # LM Studio ignores the key but the SDK requires a non-empty string
            base_url=base_url,
            model=model or os.getenv("LMSTUDIO_MODEL", DEFAULT_MODELS["lmstudio"]),
            temperature=0,
            **kwargs,
        )

    raise ValueError(f"Unknown LLM_PROVIDER: {provider!r}. Expected 'openai', 'ollama', or 'lmstudio'.")


def clean_sql(sql_text: str) -> str:
    """Remove markdown code fences if present."""
    sql = (sql_text or "").strip()
    if sql.startswith("```sql"):
        return sql.replace("```sql", "").replace("```", "").strip()
    if sql.startswith("```"):
        return sql.replace("```", "").strip()
    return sql


def make_get_schema_node(database_path: str):
    def get_schema(state: dict) -> dict:
        try:
            conn = duckdb.connect(database_path, read_only=True)
            try:
                columns = conn.execute(
                    """
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'main'
                    ORDER BY table_name, ordinal_position
                    """
                ).fetchall()
            finally:
                conn.close()

            parts = []
            current_table = None
            for table_name, column_name, data_type in columns:
                if table_name != current_table:
                    parts.append(f"\nTable: {table_name}")
                    parts.append("Columns:")
                    current_table = table_name
                parts.append(f"  - {column_name} ({data_type})")

            state["schema_info"] = "\n".join(parts).strip()
        except Exception as exc:
            state["error"] = f"Schema introspection failed: {exc}"

        return state

    return get_schema


def make_generate_sql_node(llm: ChatOpenAI):
    def generate_sql(state: dict) -> dict:
        try:
            prompt = f"""You are a SQL expert. Generate a DuckDB SQL query based on the user's question.

Database Schema:
{state['schema_info']}

Rules:
1. Only use tables and columns from the schema
2. Use DuckDB SQL syntax
3. Return ONLY SQL (no explanation)
4. Ensure SQL is valid

User Question: {state['user_question']}

SQL Query:"""
            response = llm.invoke([HumanMessage(content=prompt)])
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


def make_correct_sql_node(llm: ChatOpenAI):
    def correct_sql(state: dict) -> dict:
        try:
            state["retry_count"] = state.get("retry_count", 0) + 1

            prompt = f"""The SQL query failed with this error:
{state['execution_error']}

Failed SQL:
{state['generated_sql']}

Original question: {state['user_question']}

Database Schema:
{state['schema_info']}

This is attempt {state['retry_count']} of {MAX_RETRIES}.

Return a corrected DuckDB SQL query.
Rules:
1. Use only schema tables/columns
2. Use valid DuckDB SQL
3. Return ONLY SQL
4. Must be SELECT

Corrected SQL Query:"""

            response = llm.invoke([HumanMessage(content=prompt)])
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


def route_after_schema(state: dict) -> str:
    return "error" if state.get("error") else "ok"


def route_after_validation(state: dict) -> str:
    return "invalid" if state.get("validation_error") else "valid"


def route_after_execution(state: dict) -> str:
    if state.get("execution_error"):
        if state.get("retry_count", 0) < MAX_RETRIES:
            return "retry"
        return "max_retries"
    return "success"


def build_workflow(llm: ChatOpenAI, database_path: str):
    workflow = StateGraph(dict)

    workflow.add_node("get_schema", make_get_schema_node(database_path))
    workflow.add_node("generate_sql", make_generate_sql_node(llm))
    workflow.add_node("validate_sql", make_validate_sql_node(database_path))
    workflow.add_node("execute_query", make_execute_query_node(database_path))
    workflow.add_node("correct_sql", make_correct_sql_node(llm))
    workflow.add_node("format_results", format_results)

    workflow.set_entry_point("get_schema")
    workflow.add_conditional_edges(
        "get_schema",
        route_after_schema,
        {"ok": "generate_sql", "error": END},
    )
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
        "schema_info": "",
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
    openai_api_key: str = None,
    database_path: str = "data/askql.duckdb",
    model: str = None,
    provider: str = None,
):
    llm = build_llm(provider=provider, model=model, api_key=openai_api_key)
    workflow = build_workflow(llm, database_path)

    def run(question: str) -> Dict[str, Any]:
        final_state = workflow.invoke(initial_state(question))
        return build_result(question, final_state)

    return run, workflow


def query(
    question: str,
    openai_api_key: str = None,
    database_path: str = "data/askql.duckdb",
    model: str = None,
    provider: str = None,
) -> Dict[str, Any]:
    run, _ = create_query_runner(
        openai_api_key=openai_api_key,
        database_path=database_path,
        model=model,
        provider=provider,
    )
    return run(question)


class BasicSQLAgent:
    """Thin compatibility wrapper around function-first query pipeline."""

    def __init__(
        self,
        openai_api_key: str = None,
        database_path: str = "data/askql.duckdb",
        model: str = None,
        provider: str = None,
    ):
        self.run_query, self.workflow = create_query_runner(
            openai_api_key=openai_api_key,
            database_path=database_path,
            model=model,
            provider=provider,
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
