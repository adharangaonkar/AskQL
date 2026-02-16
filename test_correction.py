"""
Test script to verify error correction and retry logic
"""
import os
from dotenv import load_dotenv
from askQL import BasicSQLAgent

load_dotenv()

def test_error_correction():
    """Test the error correction feature"""

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("Please set OPENAI_API_KEY environment variable")
        return

    # Create agent
    agent = BasicSQLAgent(api_key)

    # Test case that will likely trigger an error that can be corrected
    # We'll ask about a table that might be misspelled
    test_cases = [
        # This should work
        "Show all products in the Electronics category",

        # This should fail validation (not SELECT)
        "Delete all customers",
    ]

    for question in test_cases:
        result = agent.query(question)

        print(f"\n{'='*60}")
        print(f"Question: {question}")
        print(f"{'='*60}")
        print(f"Success: {result['success']}")
        print(f"SQL: {result['sql']}")

        if result['success']:
            print(f"\nResults:")
            print(result['results'])
        else:
            if result['validation_error']:
                print(f"\nValidation Error: {result['validation_error']}")
            if result['execution_error']:
                print(f"\nExecution Error: {result['execution_error']}")

        if result['retry_count'] > 0:
            print(f"\nRetries: {result['retry_count']}")

        print()

if __name__ == "__main__":
    test_error_correction()
