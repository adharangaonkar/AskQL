import os

from dotenv import load_dotenv

from askQL import BasicSQLAgent

load_dotenv()


def main() -> None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Please set OPENAI_API_KEY environment variable")
        return

    agent = BasicSQLAgent(api_key)

    demo_queries = [
        "How many customers are there?",
        "Show me the top 5 most expensive products",
        "List all customers from New York",
        "What is the total revenue from all orders?",
        "Show customer names with their total spending",
    ]

    for question in demo_queries:
        result = agent.query(question)

        print("\n" + "=" * 60)
        print(f"Question: {question}")
        print("-" * 60)
        print(f"Success: {result['success']}")
        print(f"SQL: {result['sql']}")

        if result["success"]:
            print(result["results"])
            continue

        if result["validation_error"]:
            print(f"Validation Error: {result['validation_error']}")
        if result["execution_error"]:
            print(f"Execution Error: {result['execution_error']}")
        if result["error"]:
            print(f"Error: {result['error']}")


if __name__ == "__main__":
    main()
