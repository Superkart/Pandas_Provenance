import pandas as pd
from provenance.provenance_tracker import ProvenanceTracker

def main():

    tracker = ProvenanceTracker(log_File="provenance/provenance_log.json")

    df, table_name = tracker.read_csv("Pandas_Provenance_Project/color_srgb.csv")
    df2 = tracker.read_csv("Pandas_Provenance_Project/industry.csv")


    print(f"Table Name: {table_name}")
    print(f"DataFrame:\n{df}\n")

    filtered_df, filtered_table_name = tracker.filter(df, "Name=='Red'")
    print(f"Filtered Table Name: {filtered_table_name}")
    print(f"Filtered DataFrame:\n{filtered_df}\n")

    dropped_df, dropped_table_name = tracker.drop_columns(df, ["HEX"])
    print(f"Dropped Table Name: {dropped_table_name}")
    print(f"Dropped DataFrame:\n{dropped_df}\n")

    merged_df, merged_table_name = tracker.merge(df, df, on="Name")
    print(f"Merged Table Name: {merged_table_name}")
    print(f"Merged DataFrame:\n{merged_df}\n")

if __name__ == "__main__":
    main()
