import unittest
import pandas as pd
from provenance.table_utils import calculate_hash, generate_table_name

class TestTableUtils(unittest.TestCase):
    def test_calculate_hash(self):
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        hash_value = calculate_hash(df)
        self.assertTrue(isinstance(hash_value, str))

    def test_generate_table_name(self):
        hash_value = "abc123def456"
        table_name = generate_table_name(hash_value)
        self.assertEqual(table_name, "table_abc123de")

if __name__ == "__main__":
    unittest.main()
