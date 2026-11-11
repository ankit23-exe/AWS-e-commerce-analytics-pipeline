from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

import trigger_glue


def _event(key):
    return {"Records": [{"s3": {"object": {"key": key}}}]}


@pytest.fixture
def glue(monkeypatch):
    monkeypatch.setenv("GLUE_JOB_NAME", "my-job")
    client = MagicMock()
    client.start_job_run.return_value = {"JobRunId": "jr_1"}
    monkeypatch.setattr(trigger_glue, "_glue", lambda: client)
    return client


def test_ready_marker_starts_the_job(glue):
    assert trigger_glue.handler(_event("raw/_READY"), None) == {"started": ["jr_1"]}
    glue.start_job_run.assert_called_once_with(JobName="my-job")


def test_other_files_are_ignored(glue):
    assert trigger_glue.handler(_event("raw/orders/olist_orders_dataset.csv"), None) == {"started": []}
    glue.start_job_run.assert_not_called()


def test_url_encoded_keys_are_decoded(glue):
    assert trigger_glue.handler(_event("raw/%5FREADY"), None) == {"started": ["jr_1"]}


def test_concurrent_run_is_skipped_not_failed(glue):
    glue.start_job_run.side_effect = ClientError(
        {"Error": {"Code": "ConcurrentRunsExceededException", "Message": "busy"}}, "StartJobRun")
    assert trigger_glue.handler(_event("raw/_READY"), None) == {"started": []}


def test_other_errors_are_raised(glue):
    glue.start_job_run.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "StartJobRun")
    with pytest.raises(ClientError):
        trigger_glue.handler(_event("raw/_READY"), None)
