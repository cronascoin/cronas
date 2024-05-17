import subprocess

def get_short_commit_hash():
    result = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0:
        return result.stdout.strip()
    else:
        raise RuntimeError(f"Git command failed with error: {result.stderr}")

if __name__ == "__main__":
    short_commit_hash = get_short_commit_hash()
    print(short_commit_hash)
