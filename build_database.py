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
    if "hospital_stats" not in db.table_names():
        db["hospital_stats"].create({
            "id": str, "update_time": str, "description": str, 
            "commit_hexsha": str}, pk="id")
    else:
        if "commit_hexsha" not in db["hospital_stats"].columns_dict:
            db["hospital_stats"].add_column("commit_hexsha", str)

    if "hospital_record" not in db.table_names():
        db["hospital_record"].create({
            "name": str, "update_id": str, "commit_hexsha": str,
            # Add other columns based on the JSON structure
        }, pk=("name", "commit_hexsha"))
    elif "commit_hexsha" not in db["hospital_record"].columns_dict:
        db["hospital_record"].add_column("commit_hexsha", str)

def process_hospital_data(db, when, hash, content):
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
        "commit_hexsha": hash}, pk="id", replace=True)

    for hospital in hospitals:
        hospital_record = {**hospital, "update_id": update_id, "commit_hexsha": hash}
        normalized_record = {
            k.replace("\n", " ").strip(): ("" if v is None else v.replace("%", "")) 
            for k, v in hospital_record.items()}
        db["hospital_record"].upsert(normalized_record, pk=("name", "commit_hexsha"))

def export_table_to_csv(db, table_name, csv_path, order_by):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        query = db[table_name].rows_where(order_by=order_by)
        for row in query:
            if f.tell() == 0:  # Write headers only on the first line
                f.write(",".join(row.keys()) + "\n")
            f.write(",".join(['"' + str(value).replace('"', '""') + '"' 
                              for value in row.values()]) + "\n")

if __name__ == "__main__":
    db_path = "cdc.db"
    repo_path = "."  # Adjust this path as necessary
    filepaths = "hospital_data.json"
    db = sqlite_utils.Database(db_path)

    ensure_table_schema(db)

    for when, hash, content in iterate_file_versions(repo_path, (filepaths,)):
        process_hospital_data(db, when, hash, content)

    export_table_to_csv(db, "hospital_stats", "data/hospital_stats.csv", order_by="id")
    export_table_to_csv(db, "hospital_record", "data/hospital_record.csv", order_by="name, commit_hexsha")




