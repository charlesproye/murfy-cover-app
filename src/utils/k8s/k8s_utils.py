from kubernetes.client.api.core_v1_api import CoreV1Api
from kubernetes.client.models.v1_pod import V1Pod

from utils.k8s.k8s_schemas import PodStatus


def check_pod_status(
    core_api: CoreV1Api,
    driver_pod_name: str,
    namespace: str,
) -> PodStatus:
    pod: V1Pod = core_api.read_namespaced_pod(driver_pod_name, namespace)

    error_msg = None

    # Check for container errors that prevent pod from starting
    if pod.status.container_statuses:
        for container_status in pod.status.container_statuses:
            if container_status.state.waiting:
                waiting_state = container_status.state.waiting
                # Check for image pull errors and other startup failures
                if waiting_state.reason in [
                    "ErrImagePull",
                    "ImagePullBackOff",
                    "CrashLoopBackOff",
                    "CreateContainerError",
                    "InvalidImageName",
                ]:
                    error_msg = (
                        f"Driver pod {driver_pod_name} failed to start. "
                        f"Container '{container_status.name}' is in "
                        f"'{waiting_state.reason}' state. "
                        f"Message: {waiting_state.message or 'No message provided'}"
                    )
                    break

    return PodStatus(
        pod_name=driver_pod_name,
        namespace=namespace,
        status=pod.status.phase,
        error_message=error_msg,
    )
