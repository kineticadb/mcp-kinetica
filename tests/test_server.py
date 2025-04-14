import unittest
import time
from dotenv import load_dotenv
from src.server import (
    create_kinetica_client,
    list_tables,
    describe_table,
    query_sql,
    get_records,
    insert_json,
    get_sql_context, 
    start_table_monitor
)

load_dotenv()

class TestMCPKinetica(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = create_kinetica_client()
        cls.schema = "priyanshigarg068_gmail"
        cls.table = f"{cls.schema}.sample"
        cls.sample_records = [
            {"user_id": 1, "name": "Alice", "email": "alice@example.com"},
            {"user_id": 2, "name": "Bob", "email": "bob@example.com"},
        ]

    def test_list_tables(self):
        """Ensure the test table appears in the full table list."""
        tables = list_tables()
        self.assertIn(self.table, tables)

    def test_describe_table(self):
        """Verify describe_table returns valid structure for a table."""
        result = describe_table(self.table)

        self.assertNotIn("error", result, msg=f"describe_table failed: {result.get('error')}")
        self.assertIn("table_info", result)
        self.assertIn("type_info", result)

        # If type_info was returned, validate structure
        if result["type_info"]:
            self.assertIn("type_ids", result["type_info"])
            self.assertIsInstance(result["type_info"]["type_ids"], list)

    def test_get_records(self):
        """Verify that known sample records exist in the table."""
        records = get_records(self.table)

        # Check that at least 2 records exist
        self.assertGreaterEqual(len(records), 2)

        # Assert presence of sample records
        expected_users = {
            (1, "Alice", "alice@example.com"),
            (2, "Bob", "bob@example.com")
        }

        actual_users = {
            (rec["user_id"], rec["name"], rec["email"])
            for rec in records if "user_id" in rec
        }

        for user in expected_users:
            self.assertIn(user, actual_users)


    def test_query_sql_success(self):
        """Insert unique rows and verify they appear in SELECT query."""
        # Insert uniquely identifiable test records
        unique_records = [
            {"user_id": 5001, "name": "TempUserA", "email": "a@temp.com"},
            {"user_id": 5002, "name": "TempUserB", "email": "b@temp.com"},
        ]
        insert_response = insert_json(self.table, unique_records)
        print("DEBUG: Insert response:", insert_response)
        self.assertIn("data", insert_response)
        self.assertEqual(insert_response["data"]["count_inserted"], len(unique_records))

        # Query all records and verify our inserts are present
        result = query_sql(f"SELECT * FROM {self.table}")
        self.assertIn("column_1", result)
        self.assertIn("column_headers", result)

        user_ids = result["column_1"]
        self.assertIn(5001, user_ids)
        self.assertIn(5002, user_ids)



    def test_query_sql_failure(self):
        """Ensure failed queries return structured error."""
        result = query_sql("SELECT * FROM nonexistent_table_xyz")
        self.assertIn("error", result)

    def test_insert_json_isolated(self):
        """Verify insert_json handles valid payload and returns count."""
        # Insert a new isolated row with ID 9999
        new_record = [{"user_id": 9999, "name": "Charlie", "email": "charlie@example.com"}]
        response = insert_json(self.table, new_record) 
        print("DEBUG: Response from insert_json:")
        print(response)
        self.assertIn("data", response)
        self.assertIn("count_inserted", response["data"])
        self.assertGreaterEqual(response["data"]["count_inserted"], 1)

    def test_get_sql_context(self): 
        """
        Tests that the SQL context metadata for 'kgraph_ctx' is retrieved and parsed correctly.
        """
        context_name = "kgraph_ctx"
        result = get_sql_context(context_name)

        print("SQL Context Metadata:", result)

        assert isinstance(result, dict)
        assert result.get("context_name") == context_name
        assert "table" in result
        assert "comment" in result
        assert "rules" in result
        assert isinstance(result["rules"], list)

    def test_start_table_monitor_success(self):
        """
        Starts a monitor and checks the response message.
        Assumes the table already exists in the test environment.
        """
        TEST_TABLE = self.table
        response = start_table_monitor(TEST_TABLE)

        assert isinstance(response, str)
        assert "Monitoring started" in response
        assert TEST_TABLE in response


    @classmethod
    def tearDownClass(cls):
        pass  # Do not drop schema/table — reused across runs


if __name__ == "__main__":
    unittest.main()
