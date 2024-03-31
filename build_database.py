import os
import sqlite_utils
import git
import json
from datetime import datetime

def iterate_file_versions(repo_path, filepath, ref="main"):
    repo = git.Repo(repo_path)
    commits = list(repo.iter_commits(ref, paths=filepath))
    for commit in commits:
        try:
            # Ensure we're getting the correct blob for each commit
            blob = commit.tree / filepath
            yield commit.committed_date, commit.hexsha, blob.data_stream.read()
        except KeyError:
            continue  # Skip commits where the file might not be present

def ensure_table_schema(db):
    # Ensures the schema for 'hospital_stats' and 'hospital_record', adding 'commit_hexsha' if missing
    # Similar to previous implementation

def process_hospital_data(db, when, commit_hexsha, content):
    # Processes hospital data and inserts it into the database
    # Uses 'when' for 'update_id' and includes 'commit_hexsha' in each record

def export_table_to_csv(db, table_name, csv_path, order_by):
    # Exports table data to CSV
    # Similar to previous implementation

if __name__ == "__main__":
    db_path = "cdc.db"
    repo_path = "."
    filepath = "hospital_data.json"  # Assuming a single file path for clarity
    db = sqlite_utils.Database(db_path)

    ensure_table_schema(db)

    for commit_date, commit_hexsha, content in iterate_file_versions(repo_path, filepath):
        update_id = datetime.utcfromtimestamp(commit_date).strftime("%Y%m%d%H%M%S")
        process_hospital_data(db, update_id, commit_hexsha, content)

    export_table_to_csv(db, "hospital_stats", "data/hospital_stats.csv", order_by="id")
    export_table_to_csv(db, "hospital_record", "data/hospital_record.csv", order_by="name, commit_hexsha")





