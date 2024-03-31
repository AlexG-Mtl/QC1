import sqlite_utils
import git
import json
import csv
import os

def iterate_file_versions(repo_path, filepaths, ref="main"):
    repo = git.Repo(repo_path, odbt=git.GitDB)
    commits = reversed(list(repo.iter_commits(ref, paths=filepaths)))
    for commit in commits:
        blob = [b for b in commit.tree.blobs if b.name in filepaths][0]
        yield commit.committed_datetime, commit.hexsha, blob.data_stream.read()

def export_to_csv(db, table_name, output_path):
    # Ensure the directory of the output path exists, if the directory is specified
    directory = os.path.dirname(output_path)
    if directory != "":
        os.makedirs(directory, exist_ok=True)
    
    data_generator = db[table_name].rows_where(order_by="id")
    try:
        first_item = next(data_generator)
        # Need to reinitialize the generator after consuming the first item
        data_generator = db[table_name].rows_where(order_by="id")
    except StopIteration:
        print(f"No data available in table {table_name} to export.")
        return

    with open(output_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=first_item.keys())
        writer.writeheader()
        for row in data_generator:
            writer.writerow(row)

if __name__ == "__main__":
    db = sqlite_utils.Database("cdc.db")

    it = iterate_file_versions(".", ("hospital_data.json",))
    for i, (when, hash, content) in enumerate(it):
        try:
            data = json.loads(content)
            last_update = data["last_update"]
            hospitals = data["hospitals"]
        except ValueError:
            continue

        for hospital in hospitals:
            id = hash + "-" + hospital["name"]  # Unique ID combining commit hash and hospital name
            hospital_data = {
                "id": id,
                "name": hospital.get("name"),
                "address": hospital.get("address"),
                "estimated_waiting_time_non_priority": hospital.get("Estimated waiting time for non-priority cases to see a doctor"),
                "people_waiting": hospital.get("Number of people waiting to see a doctor in the emergency room"),
                "total_people": hospital.get("Total number of people in the emergency room"),
                "occupancy_rate": hospital.get("Occupancy rate of stretchers"),
                "average_waiting_room_time_previous_day": hospital.get("Average time in the waiting room (from the previous day)"),
                "average_waiting_stretcher_time_previous_day": hospital.get("Average waiting time on a stretcher (from the previous day)"),
                "last_update": last_update
            }
            db["hospital_reports"].insert(hospital_data, pk="id", alter=True, replace=True)

    # Create some indexes for better query performance
    db["hospital_reports"].create_index(["name"])
    db["hospital_reports"].create_index(["address"])

    # Export the hospital_reports table to a CSV file
    export_to_csv(db, "hospital_reports", "hospital_reports.csv")


