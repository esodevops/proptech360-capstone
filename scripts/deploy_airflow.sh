#!/usr/bin/env bash
# Run by the self-hosted runner on the Mac that runs Airflow.
set -euo pipefail

: "${PROPTECH_PROJECT_DIR:?Set the PROPTECH_PROJECT_DIR GitHub variable}"
: "${GITHUB_SHA:?Run this script through GitHub Actions}"
cd "$PROPTECH_PROJECT_DIR"

# Protect local work: deployment never resets or deletes files.
CURRENT_BRANCH="$(git branch --show-current)"
if [ "$CURRENT_BRANCH" != "main" ]; then
  echo "Deployment requires main, but the local project is on $CURRENT_BRANCH."
  echo "Switch the local project to main, then rerun the workflow."
  exit 1
fi
if [ -n "$(git status --porcelain)" ]; then
  echo "Deployment stopped because the local project has uncommitted changes:"
  git status --short
  echo "Commit or move these changes before rerunning the workflow."
  exit 1
fi

export AIRFLOW_HOME="$PROPTECH_PROJECT_DIR/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROPTECH_PROJECT_DIR/dags"
export PATH="$PROPTECH_PROJECT_DIR/venv/bin:$PATH"
test -x "$PROPTECH_PROJECT_DIR/venv/bin/python"
test -f "$PROPTECH_PROJECT_DIR/.env"

# The virtual environment contains ARM64 native packages.
if [ "$(uname -m)" != "arm64" ]; then
  echo "Use a macOS ARM64 GitHub runner. This Intel/Rosetta runner cannot use the project's ARM64 environment."
  exit 1
fi
python -c "import platform, pydantic_core; assert platform.machine() == 'arm64'"

# Do not change code while another pipeline is queued or running.
python - <<'PY'
import json
import subprocess
import sys
for state in ['running', 'queued']:
    output = subprocess.check_output([
        sys.executable, '-m', 'airflow', 'dags', 'list-runs',
        'proptech360_dag', '--state', state, '--output', 'json'
    ], text=True)
    # The final line is JSON; earlier lines may contain Airflow startup logs.
    if json.loads(output.strip().splitlines()[-1]):
        raise SystemExit('An Airflow run is active. Wait for it to finish, then rerun this workflow.')
PY

# Deploy exactly the commit that passed CI, using the existing Git credentials.
git fetch origin main
git merge-base --is-ancestor "$GITHUB_SHA" origin/main
git merge --ff-only "$GITHUB_SHA"
test "$(git rev-parse HEAD)" = "$GITHUB_SHA"
python -m pip install -r requirements.txt
python scripts/check_dag.py
python -m airflow dags reserialize
python -m airflow dags unpause proptech360_dag

RUN_ID="github_${GITHUB_RUN_ID}_${GITHUB_RUN_ATTEMPT}"
python -m airflow dags trigger proptech360_dag --run-id "$RUN_ID"
python scripts/wait_for_dag.py "$RUN_ID"
