import git
from pathlib import Path

def main():
    repo_path = '.'  # Path to your repository
    file_path = 'hospital_data.json'  # Path to the file within your repository
    
    repo = git.Repo(repo_path)
    commits = list(repo.iter_commits(paths=file_path))
    
    # Iterate over the commits in reverse order (oldest to newest)
    for i in range(len(commits)-1, 0, -1):
        commit = commits[i]
        parent_commit = commits[i-1]
        
        # Get the diffs between this commit and its parent
        diffs = parent_commit.diff(commit, paths=file_path)
        for diff in diffs:
            # Calculate additions and deletions
            additions = diff.diff.count(b'\n+') - 1  # Subtracting the diff header
            deletions = diff.diff.count(b'\n-') - 1  # Subtracting the diff header
            
            print(f"Between {parent_commit.hexsha[:7]} and {commit.hexsha[:7]}, additions: {additions}, deletions: {deletions}")

if __name__ == "__main__":
    main()
