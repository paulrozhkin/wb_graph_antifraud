import hashlib
import json
import logging
import os
import time
from datetime import date, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, urlencode
from urllib.request import urlopen

import pendulum
from airflow.sdk import dag, get_current_context, task
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


LOG = logging.getLogger(__name__)
JOB_TEMPLATE = Path(__file__).with_name("spark_job.json")


@dag(
    dag_id="daily_unique_recipients",
    schedule=os.environ["WB_GRAPH_SPARK_SCHEDULE"],
    start_date=pendulum.datetime(2026, 11, 26, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "hdfs"],
)
def daily_unique_recipients():
    @task
    def resolve_window():
        context = get_current_context()
        run = context["dag_run"]
        override = (run.conf or {}).get("feature_date")
        if override is not None:
            feature_date = date.fromisoformat(override)
        else:
            anchor = context.get("data_interval_end") or run.run_after
            feature_date = anchor.astimezone(timezone.utc).date() - timedelta(days=1)
        run_id = "af-" + hashlib.sha256(run.run_id.encode()).hexdigest()[:14]
        LOG.info("Calculating %s UTC, output run ID %s", feature_date, run_id)
        return {"feature_date": feature_date.isoformat(), "run_id": run_id}

    @task(retries=1, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(minutes=12))
    def run_spark(window):
        context = get_current_context()
        attempt = str(context["ti"].try_number)
        namespace = os.environ["WB_GRAPH_NAMESPACE"]
        job = json.loads(JOB_TEMPLATE.read_text(encoding="utf-8"))
        name = f"{job['metadata']['name']}-{window['run_id']}-{attempt}"
        labels = {"app.kubernetes.io/managed-by": "airflow", "wb-graph-run": window["run_id"]}
        job["metadata"].update(name=name, labels=labels)
        job["spec"]["ttlSecondsAfterFinished"] = 86400
        env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        for variable in env:
            if variable["name"] == "FEATURE_DATE":
                variable["value"] = window["feature_date"]
            elif variable["name"] == "FEATURE_RUN_ID":
                variable["value"] = window["run_id"]

        config.load_incluster_config()
        with client.ApiClient() as api_client:
            api = client.BatchV1Api(api_client)
            try:
                api.create_namespaced_job(namespace, job, _request_timeout=10)
                LOG.info("Started Spark Job %s/%s", namespace, name)
            except ApiException as exc:
                if exc.status != 409:
                    raise
                existing = api.read_namespaced_job(name, namespace, _request_timeout=10)
                if any((existing.metadata.labels or {}).get(key) != value for key, value in labels.items()):
                    raise RuntimeError(f"Job {namespace}/{name} is owned by another workflow") from exc
                LOG.info("Reusing Spark Job %s/%s", namespace, name)

            deadline = time.monotonic() + 660
            while time.monotonic() < deadline:
                current = api.read_namespaced_job(name, namespace, _request_timeout=10)
                conditions = current.status.conditions if current.status else None
                for condition in conditions or []:
                    if condition.status != "True":
                        continue
                    if condition.type == "Complete":
                        LOG.info("Spark Job %s/%s completed", namespace, name)
                        return window
                    if condition.type == "Failed":
                        raise RuntimeError(f"Spark Job {namespace}/{name} failed: {condition.message}")
                time.sleep(3)
            raise TimeoutError(f"Spark Job {namespace}/{name} did not complete")

    @task(retries=1, retry_delay=timedelta(seconds=15))
    def verify_publication(window):
        root = os.environ["WB_GRAPH_FEATURE_ROOT"].rstrip("/")
        path = f"{root}/unique-recipients/{window['feature_date']}/{window['run_id']}/_SUCCESS"
        query = urlencode({"op": "GETFILESTATUS", "user.name": "hadoop"})
        endpoint = os.environ["WB_GRAPH_WEBHDFS_URL"].rstrip("/")
        with urlopen(f"{endpoint}/webhdfs/v1{quote(path)}?{query}", timeout=10) as response:
            marker = json.load(response)["FileStatus"]
        if marker["type"] != "FILE":
            raise RuntimeError(f"Publication marker is not a file: {path}")
        LOG.info("Verified HDFS publication: %s", path)

    verify_publication(run_spark(resolve_window()))


daily_unique_recipients()
