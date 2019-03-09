#!/usr/bin/env python3

"""
Poll an AWS Simple Queue Services queue for messages,
and when one is received, use AppleScript to pop up an alert
with the contents of the message.
"""

import subprocess
import time

import boto3

QUEUE_NAME = 'alerts.fifo'


def alert(text):
    script = f'display notification "{text}" with title "Alert"'
    script += ' sound name "Frog"'
    cmd = ['osascript', '-e', script]
    subprocess.call(cmd)


sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)
while True:
    print(time.time())
    try:
        for message in queue.receive_messages(WaitTimeSeconds=1):
            alert(message.body)
            message.delete()
    except Exception as e:
        print(e)
        time.sleep(1)
