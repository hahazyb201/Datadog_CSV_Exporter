import argparse
from datetime import datetime, timezone
import json
import os
import csv
import re
import time
import requests

LOG_LIST_ENDPOINT = 'https://api.datadoghq.com/api/v1/logs-queries/list'
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

try:
    API_KEY = os.environ['DD_API_KEY']
    APP_KEY = os.environ['DD_APP_KEY']
except KeyError as e:
    print(f'please set {e}!')
    exit(1)

parser = argparse.ArgumentParser(description='datadog logs')
parser.add_argument('--query', required=True, dest='query')
parser.add_argument('--start', required=True, dest='start', help=f'start datetime in utc. `f{DATETIME_FORMAT}`')
parser.add_argument('--end', required=True, dest='end', help=f'start datetime in utc. `f{DATETIME_FORMAT}`')
parser.add_argument('--limit', required=False, default=1_000, dest='limit')
parser.add_argument('--filter', required=False, action='append', dest='filter')
parser.add_argument('--output', required=False, default='output', dest='output')

def parse_datetime_str(s: str) -> datetime:
    return datetime.strptime(s, DATETIME_FORMAT)

def datetime_to_str(d: datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def call_list_logs_api(payload: dict) -> dict:
    res = requests.post(
        LOG_LIST_ENDPOINT,
        params={
            'api_key': API_KEY,
            'application_key': APP_KEY
        },
        json=payload
    )
    res.raise_for_status()
    return res.json()

def list_logs(
        query: str,
        start: datetime,
        end: datetime,
        limit: int = 1_000):
    payload = {
        'query': query,
        'time': {
            'from': datetime_to_str(start),
            'to': datetime_to_str(end)
        },
        'limit': limit
    }
    res = call_list_logs_api(payload)
    logs = res.get('logs')
    next_log_id = res.get('nextLogId')
    while next_log_id:
        next_payload = {**payload, 'startAt': next_log_id}
        res = call_list_logs_api(next_payload)
        logs += res.get('logs', [])
        next_log_id = res.get('nextLogId')
    return logs

def save_logs(
        query: str,
        start: datetime,
        end: datetime,
        output: str,
        limit: int = 1_000):
    logs = list_logs(query, start, end, limit)
    destination_file = output + '_' + time.strftime("%Y-%m-%d", time.localtime()) + '.csv'
    with open(destination_file, 'w') as write_file: 
        csv_writer = csv.writer(write_file) 
        for log in logs:
            params = re.findall(r"\[(\d+)\]", log['content']['message'])
            csv_writer.writerow(params) 

if __name__ == "__main__":
    args = parser.parse_args()
    start_datetime = parse_datetime_str(args.start)
    end_datetime = parse_datetime_str(args.end)
    save_logs(args.query, start_datetime, end_datetime, args.output, limit=args.limit)
