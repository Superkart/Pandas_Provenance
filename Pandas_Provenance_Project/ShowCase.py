import os
import json
from provenance.provenance_tracker import ProvenanceTracker

# Provide the path to your CSV file
csv_file_path = os.path.abspath("Pandas_Provenance_Project/color_srgb.csv")

def main():
    tracker = ProvenanceTracker()

    print("== Read CSV ==")
    df, table_name = tracker.read_csv(csv_file_path)
    print(f"DataFrame: \n{df.head()}")
    print(f"Table Name: {table_name}")

    print("\n== Filter Operation ==")
    filtered_df, filtered_table_name = tracker.filter(df, "Name=='Red'")
    print(f"Filtered DataFrame: \n{filtered_df}")
    print(f"Filtered Table Name: {filtered_table_name}")

    print("\n== Drop Columns Operation ==")
    dropped_df, dropped_table_name = tracker.drop_columns(df, ["HEX"])
    print(f"Dropped Columns DataFrame: \n{dropped_df.head()}")
    print(f"Dropped Table Name: {dropped_table_name}")

    print("\n== Merge Operation ==")
    merged_df, merged_table_name = tracker.merge(df, df, on="Name")
    print(f"Merged DataFrame: \n{merged_df.head()}")
    print(f"Merged Table Name: {merged_table_name}")

    print("\n== Provenance Logs ==")
    with open("./Pandas_Provenance_Project/provenance/provenance_log.json", "r") as f:
        logs = json.load(f)
        print(json.dumps(logs, indent=4))

if __name__ == "__main__":
    main()
