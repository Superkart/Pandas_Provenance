import sys
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.Pandas_Provenance import ProvenanceTracker

tracker = ProvenanceTracker()

df = pd.DataFrame({'A': [1, 2, 3, 4, 5], 'B': [10, 20, 30, 40, 50]})

df, table_name = tracker.track_table_transformation(df, source_file='data/sample_data.csv', transformation_type='read_csv')
print(f"Original DataFrame: {table_name}")
print(df)

filtered_df, filter_table_name = tracker.filter(df, 'A > 2')
print(f"\nFiltered DataFrame: {filter_table_name}")
print(filtered_df)

df_dropped, drop_table_name = tracker.drop_columns(filtered_df, ['B'])
print(f"\nDataFrame after dropping column B: {drop_table_name}")
print(df_dropped)

df2 = pd.DataFrame({'A': [3, 4, 5, 6], 'C': ['a', 'b', 'c', 'd']})

merged_df, merge_table_name = tracker.merge(df_dropped, df2, on='A')
print(f"\nMerged DataFrame: {merge_table_name}")
print(merged_df)

print("\nProvenance log has been updated with all operations.")