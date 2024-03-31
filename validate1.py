import git

def main():
    repo_path = '.'
    file_path = 'hospital_data.json'
    repo = git.Repo(repo_path)
    # Fetch commits affecting the file
    commits = list(repo.iter_commits(paths=file_path))[:2]  # Get the last 2 commits

    if len(commits) < 2:
        print("Not enough commit history found for comparison.")
        return

    commit = commits[0]
    parent_commit = commits[1]  # Direct parent in a linear history

    diffs = parent_commit.diff(commit, paths=file_path)
    for diff in diffs:
        additions = diff.diff.count(b'\n+') - 1
        deletions = diff.diff.count(b'\n-') - 1
        print(f"Between commits {parent_commit.hexsha[:7]} and {commit.hexsha[:7]}, additions: {additions}, deletions: {deletions}")

if __name__ == "__main__":
    main()


