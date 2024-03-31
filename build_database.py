import os
import sqlite_utils
import git
import json
from datetime import datetime

def iterate_file_versions(repo_path, filepaths, ref="main"):
    repo = git.Repo(repo_path, odbt=git.GitDB)
    commits = reversed(list(repo.iter_commits(ref, paths=filepaths)))
    for commit in commits:
        blob = [b for b in commit.tree.blobs if b.name in filepaths][0]
        yield commit.committed_datetime, commit.hexsha, blob.data_stream.read()

def ensure_table_schema(db):
    # Assuming previous code for schema assurance was here, properly indented

# Make sure there is no unindented code here before the next function definition

def process_hospital_data(db, when, commit_hexsha, content):
    try:
        data = json.loads(content)
    except ValueError:
        return  # Skip if not valid JSON

    last_update = data["last_update"]
    update_id = when.strftime("%Y%m%d%H%M%S")
    hospitals = data.get("hospitals", [])
    
    db["hospital_stats"].insert({
        "id": update_id, "update_time": last_update, 
        "description": "Hospital data update", 
        "commit_hexsha": commit_hexsha}, pk="id", replace=True)

    for hospital in hospitals:
        hospital_record = {**hospital, "update_id": update_id, "commit_hexsha": commit_hexsha}
        normalized_record = {
            k.replace("\n", " ").strip(): ("" if v is None else v.replace("%", "")) 
            for k, v in hospital_record.items()}
        db["hospital_record"].upsert(normalized_record, pk=("name", "commit_hexsha"))

# Further code...

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





