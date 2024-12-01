import unittest
import pandas as pd
from provenance.provenance_tracker import ProvenanceTracker

class TestProvenanceTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = ProvenanceTracker(log_file="test_log.json")
        self.df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

    def test_read_csv(self):
        df, _ = self.tracker.track_table(self.df, operation="test_operation")
        self.assertEqual(len(self.tracker.logs), 1)

    def tearDown(self):
        import os
        os.remove("test_log.json")

if __name__ == "__main__":
    unittest.main()
