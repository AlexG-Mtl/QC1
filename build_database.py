import os  
import sqlite_utils
import git
import json
from datetime import datetime

# The rest of your script follows...


def iterate_file_versions(repo_path, filepaths, ref="main"):
    repo = git.Repo(repo_path, odbt=git.GitDB)
    commits = reversed(list(repo.iter_commits(ref, paths=filepaths)))
    for commit in commits:
        blob = [b for b in commit.tree.blobs if b.name in filepaths][0]
        yield commit.committed_datetime, commit.hexsha, blob.data_stream.read()

def process_hospital_data(db, content):
    try:
        data = json.loads(content)
    except ValueError:
        # Skip if the content is not valid JSON
        return
    
    last_update = data["last_update"]
    hospitals = data.get("hospitals", [])

    # Generate a unique ID for this update
    update_id = datetime.now().strftime("%Y%m%d%H%M%S")

    # Insert/update last_update info into hospital_stats
    db["hospital_stats"].insert({"id": update_id, "update_time": last_update, "description": "Hospital data update"}, pk="id", replace=True)

    # Process each hospital entry
    for hospital in hospitals:
        hospital_record = {**hospital, "update_id": update_id}
        # Normalize the keys and values as needed
        normalized_record = {k.replace("\n", " ").strip(): (v if v is None or "%" not in v else v.replace("%", "")) for k, v in hospital_record.items()}
        db["hospital_record"].insert(normalized_record, pk="name", alter=True, replace=True)

def export_table_to_csv(db, table_name, csv_path):
    # Ensure directory exists (Python 3.5+)
    import os
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

def export_table_to_csv(db, table_name, csv_path, order_by=None):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        # Adjusting the rows_where call based on the order_by parameter
        query = db[table_name].rows_where(order_by=order_by) if order_by else db[table_name].rows_where()
        for row in query:
            if f.tell() == 0:  # Write headers only on the first line
                f.write(",".join(row.keys()) + "\n")
            f.write(",".join(['"' + str(value).replace('"', '""') + '"' for value in row.values()]) + "\n")


if __name__ == "__main__":
    db_path = "cdc.db"
    repo_path = "."  # Adjust this path as necessary
    filepaths = "hospital_data.json"
    db = sqlite_utils.Database(db_path)

    # Iterate through file versions
    for when, hash, content in iterate_file_versions(repo_path, (filepaths,)):
        process_hospital_data(db, content)

    # Export tables to CSV files using the helper function
    export_table_to_csv(db, "hospital_stats", "data/hospital_stats1.csv")
    export_table_to_csv(db, "hospital_record", "data/hospital_record1.csv", order_by="name")  # Assuming 'name' is the correct column

