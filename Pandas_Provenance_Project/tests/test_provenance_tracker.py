import pandas as pd
from provenance.provenance_tracker import ProvenanceTracker


def test_provenance_tracking():
    tracker = ProvenanceTracker(log_file="test_log.json")
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

    table_name = tracker.track_table(df)
    assert table_name in tracker.provenance_log

    provenance = tracker.get_provenance(table_name)
    assert provenance["columns"] == ["A", "B"]

def test_why_provenance():
    tracker = ProvenanceTracker()
    df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

    df_with_why = tracker.add_why_provenance(df)
    assert "why" in df_with_why.columns
    assert isinstance(df_with_why["why"][0], set)
