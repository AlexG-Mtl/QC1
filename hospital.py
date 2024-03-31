import sqlite_utils
import git
import json
import csv

def iterate_file_versions(repo_path, filepaths, ref="main"):
    repo = git.Repo(repo_path, odbt=git.GitDB)
    commits = reversed(list(repo.iter_commits(ref, paths=filepaths)))
    for commit in commits:
        blob = [b for b in commit.tree.blobs if b.name in filepaths][0]
        yield commit.committed_datetime, commit.hexsha, blob.data_stream.read()

def export_to_csv(db, table_name, output_path):
    data = db[table_name].rows_where(order_by="id")
    with open(output_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        for row in data:
            writer.writerow(row)

if __name__ == "__main__":
    db = sqlite_utils.Database("cdc.db")
    # Your data processing logic here
    
    # After processing, export the hospital_reports table to a CSV file
    export_to_csv(db, "hospital_reports", "hospital_reports.csv")
    print("Exported hospital data to 'hospital_reports.csv'")
