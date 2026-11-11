"""Lambda: start the Glue job when the `_READY` marker lands in the raw zone.

Why a marker file? Uploading 6 CSVs would otherwise fire 6 events and start 6 job runs.
Upload the data first, then upload an empty `raw/_READY` file to start one run.
"""
import logging
import os
import urllib.parse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

READY_MARKER = "_READY"
_glue_client = None


def _glue():
    global _glue_client
    if _glue_client is None:
        _glue_client = boto3.client("glue")
    return _glue_client


def handler(event, context):
    job_name = os.environ["GLUE_JOB_NAME"]
    started = []
    for record in event.get("Records", []):
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        if not key.endswith(READY_MARKER):
            logger.info("Ignoring %s (not the ready marker)", key)
            continue
        try:
            run = _glue().start_job_run(JobName=job_name)
            logger.info("Started %s run %s", job_name, run["JobRunId"])
            started.append(run["JobRunId"])
        except ClientError as err:
            if err.response["Error"]["Code"] == "ConcurrentRunsExceededException":
                logger.warning("A run of %s is already in progress, skipping", job_name)
            else:
                raise
    return {"started": started}
