#!/usr/bin/env python3

"""
Monitor jobs for brokenness, and when brokenness is detected, send a message to an AWS SQS queue.

Supports two types of job monitoring:
* AWS Batch jobs (--aws_batch_jobs).
  Reports jobs in FAILED state.
* Local jobs (--local_runs_dir <dir>).
  Reads all .log files in the directory, looking for strings specified in `include` and `exclude` below.

We only want to send a single message about each broken job, so we store state in a file `state`
in the current directory.
"""

import argparse
import glob
import json
import os
import subprocess
import uuid
from pathlib import Path
from socket import gethostname

import requests

# AWS SQS URL
queue_url = "https://.../alerts.fifo"

# Log file strings which cause us to cause us to consider a local job broken
include = ['Exception']
# But if we see a line like "Exception: HTTPError", then it's OK; don't consider the job broken
exclude = ['HTTPError']


def send_alert(text):
    global data
    data = {'Action': 'SendMessage',
            'MessageBody': text,
            'MessageGroupId': '0',
            'MessageDeduplicationId': str(int(uuid.uuid4()))}
    requests.post(queue_url, data=data).raise_for_status()


def mark_as_broken(state_path, run_name):
    with open(state_path, 'a') as f:
        f.write(run_name + '\n')


def check_logs(dirs, state_path, already_broken_runs):
    for run_dir in dirs:
        run_name = Path(run_dir).parts[-1]
        if run_name in already_broken_runs:
            continue

        error_lines = []
        for log_file in glob.glob(os.path.join(run_dir, '*.log')):
            with open(log_file, 'r') as f:
                lines = f.readlines()
            for line in lines:
                if any([p in line for p in include]) and not any([p in line for p in exclude]):
                    error_lines.append(line)

        if error_lines:
            send_alert(f"Host {gethostname()} run {run_name} broken")
            mark_as_broken(state_path, run_name)


def check_jobs(state_path, already_broken_runs):
    json_output = subprocess.check_output("aws batch list-jobs --job-queue q --job-status FAILED", shell=True)
    for job in json.loads(json_output)['jobSummaryList']:
        if job['jobId'] in already_broken_runs:
            continue
        mark_as_broken(state_path, job['jobId'])
        send_alert(f"AWS run {job['jobName']} ({job['jobId']}) failed")


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_argument_group()
    group.add_argument('--aws_batch_jobs', action='store_true')
    group.add_argument('--local_runs_dir')
    args = parser.parse_args()

    state_dir = os.path.dirname(os.path.abspath(__file__))
    state_path = os.path.join(state_dir, 'state')
    if os.path.exists(state_path):
        with open(state_path, 'r') as f:
            already_broken_runs = [l.strip() for l in f.readlines()]
    else:
        already_broken_runs = []

    if args.aws_batch_jobs:
        check_jobs(state_path, already_broken_runs)
    elif args.local_runs_dir:
        paths = [os.path.join(args.local_runs_dir, p) for p in os.listdir(args.local_runs_dir)]
        dirs = [p for p in paths if os.path.isdir(p)]

        check_logs(dirs, state_path, already_broken_runs)
    else:
        raise Exception("No check mode specified")


if __name__ == '__main__':
    main()
