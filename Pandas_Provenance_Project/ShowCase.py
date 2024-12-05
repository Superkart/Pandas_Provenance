import pandas as pd
from provenance.provenance_tracker import ProvenanceTracker

def main():
    # Initialize the ProvenanceTracker with the relative path to the provenance_log.json
    tracker = ProvenanceTracker(log_file="provenance/provenance_log.json")

    # Read the CSV file and track the provenance
    df, table_name = tracker.read_csv("color_srgb.csv")
    df2 = tracker.read_csv("industry.csv")


    print(f"Table Name: {table_name}")
    print(f"DataFrame:\n{df}\n")

    # Example: Filter operation - filter for rows where 'Name' is 'Red'
    filtered_df, filtered_table_name = tracker.filter(df, "Name=='Red'")
    print(f"Filtered Table Name: {filtered_table_name}")
    print(f"Filtered DataFrame:\n{filtered_df}\n")

    # Example: Drop column operation - drop the 'HEX' column
    dropped_df, dropped_table_name = tracker.drop_columns(df, ["HEX"])
    print(f"Dropped Table Name: {dropped_table_name}")
    print(f"Dropped DataFrame:\n{dropped_df}\n")

    # Example: Merge operation - merge the dataframe with itself on 'Name' column
    merged_df, merged_table_name = tracker.merge(df, df, on="Name")
    print(f"Merged Table Name: {merged_table_name}")
    print(f"Merged DataFrame:\n{merged_df}\n")

if __name__ == "__main__":
    main()
