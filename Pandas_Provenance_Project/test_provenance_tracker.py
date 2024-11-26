import pandas as pd
from provenance_tracker import ProvenanceTracker

def test_provenance_logging():
    tracker = ProvenanceTracker()
    df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    tracker.track_table(df, 'test_table')
    assert len(tracker.provenance_log) == 1
    assert tracker.provenance_log[0]['table_name'] == 'test_table'
