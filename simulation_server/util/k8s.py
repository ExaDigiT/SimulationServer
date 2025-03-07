import kubernetes as k8s
import functools
from pathlib import Path

@functools.cache
def get_batch_api():
    k8s.config.load_incluster_config()
    return k8s.client.BatchV1Api()


@functools.cache
def get_namespace():
    return Path('/var/run/secrets/kubernetes.io/serviceaccount/namespace').read_text().strip()


def submit_job(job: dict):
    return get_batch_api().create_namespaced_job(namespace = get_namespace(), body = job)


def get_job(name: str):
    try:
        return get_batch_api().read_namespaced_job(namespace = get_namespace(), name = name)
    except k8s.client.ApiException as e:
        if e.status == 404:
            return None
        else:
            raise e


def get_job_state(job):
    if job:
        if job.status.succeeded:
            return 'success'
        elif not job.status.active and job.status.failed:
            return 'fail'
        else:
            return 'running'
    else:
        return 'deleted'


def get_job_end_time(job):
    # completion_time for failed jobs is null
    return job.status.completion_time or job.status.conditions[-1].last_transition_time
