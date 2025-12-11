#!/usr/bin/env bash
set -euo pipefail

# Get the current directory name (repo name)
REPO_NAME="$(basename "$(pwd)")"

echo "Initializing new git repo for: $REPO_NAME"

git init
git add .
git commit -m "first commit"
git branch -M main

# Construct remote URL
REMOTE_URL="https://github.com/gar2000b/${REPO_NAME}.git"
echo "Setting remote to: $REMOTE_URL"

git remote add origin "$REMOTE_URL"
git push -u origin main

echo "✔ Repo initialized and pushed successfully."

