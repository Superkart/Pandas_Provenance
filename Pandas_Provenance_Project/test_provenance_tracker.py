import pandas as pd

from src.Pandas_Provenance import ProvenanceTracker


def _tracker(tmp_path):
    return ProvenanceTracker(log_file=str(tmp_path / "provenance_log.json"))


def test_read_csv_registers_source_provenance(tmp_path):
    csv_path = tmp_path / "input.csv"
    pd.DataFrame({"A": [1, 2], "B": [10, 20]}).to_csv(csv_path, index=False)

    tracker = _tracker(tmp_path)
    df, _ = tracker.read_csv(str(csv_path))

    table_why = tracker.get_table_why_provenance(df)
    assert set(table_why.keys()) == {"0", "1"}
    assert table_why["0"]["source"] == "read"
    assert table_why["0"]["witness_sets"][0][0]["row_index"] == 0


def test_filter_keeps_identity_witness_sets(tmp_path):
    tracker = _tracker(tmp_path)
    df = pd.DataFrame({"A": [1, 2, 3], "B": [10, 20, 30]})
    df, _ = tracker.track_table_transformation(df, transformation_type="source")

    filtered, _ = tracker.filter(df, "A > 1")

    row_1_why = tracker.get_row_why_provenance(filtered, 1)
    row_2_why = tracker.get_row_why_provenance(filtered, 2)
    assert row_1_why["witness_sets"][0][0]["row_index"] == 1
    assert row_2_why["witness_sets"][0][0]["row_index"] == 2


def test_drop_columns_preserves_row_lineage(tmp_path):
    tracker = _tracker(tmp_path)
    df = pd.DataFrame({"A": [5, 6], "B": [50, 60]})
    df, _ = tracker.track_table_transformation(df, transformation_type="source")

    dropped, _ = tracker.drop_columns(df, ["B"])

    assert list(dropped.columns) == ["A"]
    row_0_why = tracker.get_row_why_provenance(dropped, 0)
    assert row_0_why["witness_sets"][0][0]["row_index"] == 0


def test_merge_combines_witness_sets_from_both_inputs(tmp_path):
    tracker = _tracker(tmp_path)
    left = pd.DataFrame({"A": [2, 3], "B": [20, 30]})
    right = pd.DataFrame({"A": [3, 4], "C": ["x", "y"]})

    left, _ = tracker.track_table_transformation(left, transformation_type="source")
    right, _ = tracker.track_table_transformation(right, transformation_type="source")

    merged, _ = tracker.merge(left, right, on="A")

    assert len(merged) == 1
    row_0_why = tracker.get_row_why_provenance(merged, 0)
    assert row_0_why["source"] == "merge"
    witness = row_0_why["witness_sets"][0]
    assert len(witness) == 2
    assert {item["row_index"] for item in witness} == {1, 0}
