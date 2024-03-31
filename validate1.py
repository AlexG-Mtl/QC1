import git
from pathlib import Path
import json

def main():
    repo_path = '.'  # Adjust as necessary
    file_path = 'hospital_data.json'  # Adjust as necessary
    repo = git.Repo(repo_path)
    commits = list(repo.iter_commits(paths=file_path))
    print(f"Found {len(commits)} commits affecting {file_path}.")

    for i in range(len(commits)-1, 0, -1):
        commit = commits[i]
        parent_commit = commits[i-1]
        diffs = parent_commit.diff(commit, paths=file_path)
        for diff in diffs:
            additions = diff.diff.count(b'\n+') - 1
            deletions = diff.diff.count(b'\n-') - 1
            print(f"Commit {commit.hexsha[:7]}: +{additions} -{deletions}")

if __name__ == "__main__":
    main()

