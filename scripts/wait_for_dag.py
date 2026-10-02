"""Report failure to GitHub if the triggered Airflow run fails or times out."""
import json
import subprocess
import sys
import time

run_id = sys.argv[1]
for attempt in range(60):  # Wait up to 15 minutes.
    output = subprocess.check_output([
        sys.executable, '-m', 'airflow', 'dags', 'list-runs',
        'proptech360_dag', '--output', 'json'
    ], text=True)
    # Airflow writes startup logs before its final JSON result line.
    runs = json.loads(output.strip().splitlines()[-1])
    for run in runs:
        if run['run_id'] == run_id:
            print(f"{run_id}: {run['state']}", flush=True)
            if run['state'] == 'success':
                sys.exit(0)
            if run['state'] == 'failed':
                sys.exit('Airflow failed. Open the task logs in the Airflow UI.')
    time.sleep(15)
sys.exit('Timed out waiting for Airflow. Check the scheduler and task logs; the run may still be active.')
