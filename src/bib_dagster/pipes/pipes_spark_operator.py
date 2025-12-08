"""Dagster Pipes client for Kubeflow Spark Operator SparkApplications."""

import importlib
import os
import random
import re
import string
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Literal

import kubernetes
from dagster import OpExecutionContext
from dagster import _check as check
from dagster._annotations import public
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.execution.context.asset_execution_context import (
    AssetExecutionContext,
)
from dagster._core.pipes.client import (
    PipesClient,
    PipesClientCompletedInvocation,
    PipesContextInjector,
    PipesMessageReader,
)
from dagster._core.pipes.context import PipesMessageHandler
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    PipesTempFileMessageReader,
    extract_message_or_forward_to_stdout,
    open_pipes_session,
)
from dagster_k8s.utils import detect_current_namespace
from dagster_pipes import PipesDefaultMessageWriter

from bib_dagster.pipes.spark_resources import DriverResource, ExecutorResource
from utils.k8s import k8s_utils

DEFAULT_WAIT_TIMEOUT = 3 * 60 * 60  # 3 hours
DEFAULT_POLL_INTERVAL = 5.0  # 5 seconds
DEFAULT_MAX_WAIT = 3600  # 1 hour, can be very long when cluster under load
SPARK_HISTORY_URL_TEMPLATE = (
    "https://spark-history.bibers.fr/history/{spark_app_id}/jobs/"
)


def get_spark_app_name(
    context: OpExecutionContext | AssetExecutionContext,
) -> str:
    """Generate a valid SparkApplication name from context, including asset name and partition.

    Args:
        context: Dagster execution context

    Returns:
        DNS-1123 compliant name (max 63 chars)
    """
    run_id = context.run_id
    op_name = context.op.name

    # Extract asset name and partition if available
    asset_name = None
    partition = None
    if isinstance(context, AssetExecutionContext):
        # Get asset name from the first asset key
        if context.assets_def.keys:
            asset_name = next(iter(context.assets_def.keys)).path[-1]
        partition = context.partition_key if context.has_partition_key else None

    # Use asset name if available, otherwise fall back to op_name
    base_name = asset_name if asset_name else op_name
    clean_name = re.sub("[^a-z0-9-]", "", base_name.lower().replace("_", "-"))

    # Clean partition string if present
    clean_partition = ""
    if partition:
        clean_partition = re.sub("[^a-z0-9-]", "", partition.lower().replace("_", "-"))
        # Limit partition to reasonable length
        clean_partition = f"-{clean_partition[:15]}"

    suffix = "".join(
        random.choice(string.ascii_lowercase + string.digits) for i in range(6)
    )

    # SparkApplication names must be DNS-1123 compliant (max 63 chars)
    # Format: dagster-{run_id}-{name}-{partition}-{suffix}
    # Allocate space: "dagster-" (3) + run_id (8) + "-" (1) + name (20) + partition (16) + "-" (1) + suffix (6)

    return f"dg-{run_id[:8]}-{clean_name[:25]}{clean_partition}-{suffix}"[:63]


class PipesSparkApplicationLogsMessageReader(PipesMessageReader):
    """Message reader that reads messages from SparkApplication driver pod logs."""

    @contextmanager
    def read_messages(
        self,
        handler: PipesMessageHandler,
    ) -> Iterator[dict[str, Any]]:
        self._handler = handler
        try:
            yield {
                PipesDefaultMessageWriter.STDIO_KEY: PipesDefaultMessageWriter.STDERR
            }
        finally:
            self._handler = None

    def consume_driver_logs(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        core_api: kubernetes.client.CoreV1Api,
        driver_pod_name: str,
        namespace: str,
        max_wait: int = DEFAULT_MAX_WAIT,
    ):
        """Consume logs from the Spark driver pod."""
        handler = check.not_none(
            self._handler, "can only consume logs within scope of context manager"
        )

        try:
            # Wait for driver pod to be ready
            context.log.info(f"Waiting for driver pod {driver_pod_name} to be ready...")
            start_time = time.time()
            is_pod_ready = False

            while time.time() - start_time < max_wait:
                pod_status = k8s_utils.check_pod_status(
                    core_api=core_api,
                    driver_pod_name=driver_pod_name,
                    namespace=namespace,
                )
                is_pod_ready = pod_status.is_ready
                if pod_status.error_message:
                    raise RuntimeError(
                        f"Driver pod {driver_pod_name} is not ready: {pod_status.error_message}"
                    )
                if is_pod_ready:
                    break

                time.sleep(2)
            else:
                # Timeout reached without pod becoming ready
                raise TimeoutError(
                    f"Driver pod {driver_pod_name} did not become ready within {max_wait} seconds"
                )

            # Stream logs
            context.log.info(f"Streaming logs from driver pod {driver_pod_name}...")
            log_stream = core_api.read_namespaced_pod_log(
                driver_pod_name,
                namespace,
                follow=True,
                _preload_content=False,
            )

            for line in log_stream:
                message = line.decode("utf-8").rstrip()
                extract_message_or_forward_to_stdout(handler, message)
        except Exception as e:
            context.log.error(
                f"Unexpected error reading driver logs: {e!s}", exc_info=True
            )
            raise

    def no_messages_debug_text(self) -> str:
        return "Attempted to read messages from SparkApplication driver pod logs."


@public
class PipesSparkApplicationClient(PipesClient, TreatAsResourceParam):
    """A Pipes client for running Spark workloads via Kubeflow Spark Operator.

    This client creates and manages SparkApplication custom resources, enabling
    Dagster to orchestrate Spark jobs on Kubernetes with full Pipes protocol support.

    Args:
        load_incluster_config (Optional[bool]): Whether to load in-cluster Kubernetes config.
            If True, uses kubernetes.config.load_incluster_config().
            If False or None, uses kubernetes.config.load_kube_config().
            Default: None (auto-detect based on KUBERNETES_SERVICE_HOST env var).
        kubeconfig_file (Optional[str]): Path to kubeconfig file when using
            load_kube_config. Default: None.
        kube_context (Optional[str]): Kubernetes context name when using
            load_kube_config. Default: None.
        context_injector (Optional[PipesContextInjector]): Context injector for Pipes protocol.
            Defaults to PipesEnvContextInjector.
        message_reader (Optional[PipesMessageReader]): Message reader for Pipes protocol.
            Defaults to PipesSparkApplicationLogsMessageReader.
        forward_termination (bool): Whether to delete the SparkApplication when
            Dagster process is interrupted. Default: True.
        inject_method (Literal["env", "conf"]): How to inject Pipes context into Spark.
            "env" uses driver/executor environment variables.
            "conf" uses Spark configuration properties.
            Default: "env".
    """

    def __init__(
        self,
        load_incluster_config: bool | None = None,
        kubeconfig_file: str | None = None,
        kube_context: str | None = None,
        context_injector: PipesContextInjector | None = None,
        message_reader: PipesMessageReader | None = None,
        forward_termination: bool = True,
        inject_method: Literal["env", "conf"] = "env",
    ):
        self.load_incluster_config: bool = check.opt_bool_param(
            load_incluster_config, "load_incluster_config"
        )
        self.kubeconfig_file = check.opt_str_param(kubeconfig_file, "kubeconfig_file")
        self.kube_context = check.opt_str_param(kube_context, "kube_context")
        self.context_injector = (
            context_injector
            if context_injector is not None
            else PipesEnvContextInjector()
        )
        self.message_reader = (
            message_reader
            if message_reader is not None
            else PipesSparkApplicationLogsMessageReader()
        )
        self.forward_termination = check.bool_param(
            forward_termination, "forward_termination"
        )
        self.inject_method = inject_method

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def _load_k8s_config(self):
        """Load Kubernetes configuration."""
        if (
            self.load_incluster_config is None
            and self.kubeconfig_file is None
            and self.kube_context is None
        ):
            # Auto-detect: use in-cluster if KUBERNETES_SERVICE_HOST is set
            if os.getenv("KUBERNETES_SERVICE_HOST"):
                kubernetes.config.load_incluster_config()
            else:
                kubernetes.config.load_kube_config()
        elif self.load_incluster_config:
            kubernetes.config.load_incluster_config()
        else:
            kubernetes.config.load_kube_config(
                config_file=self.kubeconfig_file,
                context=self.kube_context,
            )

    def _enrich_spark_app_spec(
        self,
        base_spec: dict[str, Any],
        env_vars: dict[str, str],
    ) -> dict[str, Any]:
        """Enrich SparkApplication spec with Pipes context."""
        spec = base_spec.copy()

        if self.inject_method == "env":
            # Inject via environment variables
            if "driver" not in spec:
                spec["driver"] = {}
            if "executor" not in spec:
                spec["executor"] = {}

            # Add env vars to driver
            if "env" not in spec["driver"]:
                spec["driver"]["env"] = []
            for key, value in env_vars.items():
                spec["driver"]["env"].append({"name": key, "value": value})

            # Add context env var to executors (messages go to /dev/null)
            if "env" not in spec["executor"]:
                spec["executor"]["env"] = []
            context_var = next(
                (
                    item
                    for item in spec["driver"]["env"]
                    if "DAGSTER_PIPES_CONTEXT" in item["name"]
                ),
                None,
            )
            if context_var:
                spec["executor"]["env"].append(context_var)

        elif self.inject_method == "conf":
            # Inject via Spark configuration
            if "sparkConf" not in spec:
                spec["sparkConf"] = {}
            for key, value in env_vars.items():
                conf_key = f"spark.executorEnv.{key}"
                spec["sparkConf"][conf_key] = value

        return spec

    @public
    def run(
        self,
        *,
        context: OpExecutionContext | AssetExecutionContext,
        base_spec: dict[str, Any],
        namespace: str | None = None,
        extras: dict[str, Any] | None = None,
        cleanup: bool = True,
        driver_resource: DriverResource | None = None,
        executor_resource: ExecutorResource | None = None,
    ) -> PipesClientCompletedInvocation:
        """Run a Spark job via SparkApplication or locally based on DAGSTER_ENV.

        Automatically detects the environment:
        - DAGSTER_ENV=dev (or unset): Runs locally with Python PySpark
        - DAGSTER_ENV=prod: Runs on Kubernetes via Spark Operator

        Args:
            context (Union[OpExecutionContext, AssetExecutionContext]): Dagster execution context.
            base_spec (dict[str, Any]): Base SparkApplication spec. Should include at minimum:
                - arguments: List with ["run", "make_name"] for local execution
                - type: Python or Scala (for Kubernetes execution)
                - image: Docker image for driver/executor (for Kubernetes execution)
                - mainApplicationFile: Path to main Spark application file (for Kubernetes execution)
                - sparkVersion: Spark version (e.g., "3.5.0") (for Kubernetes execution)
                Other fields like driver, executor, sparkConf can be provided and will be merged
                with Pipes-specific configuration.
            namespace (Optional[str]): Kubernetes namespace. Defaults to current namespace or "default".
            extras (Optional[dict[str, Any]]): Additional Pipes extras to pass to the Spark job.
            cleanup (bool): Whether to cleanup resources after execution (Kubernetes only).

        Returns:
            PipesClientCompletedInvocation: Completed invocation with results from Spark job.

        Example:
            ```python
            from dagster import asset
            from bib_dagster.pipes import PipesSparkApplicationClient

            @asset
            def my_spark_asset(
                context,
                spark_pipes: PipesSparkApplicationClient,
            ):
                return spark_pipes.run(
                    context=context,
                    base_spec={
                        "arguments": ["run", "bmw"],  # For local execution
                        "type": "Python",  # For Kubernetes execution
                        "pythonVersion": "3",
                        "mode": "cluster",
                        "image": "my-spark:latest",
                        "mainApplicationFile": "local:///app/my_job.py",
                        "sparkVersion": "3.5.0",
                        "driver": {"cores": 1, "memory": "1g"},
                        "executor": {"cores": 2, "instances": 2, "memory": "2g"},
                    },
                    namespace="spark-operator",
                )
            ```
        """
        # Check environment to determine execution mode
        env = os.getenv("DAGSTER_ENV", "dev").lower()

        if env == "dev":
            # Run locally with direct Python imports and local Spark
            return self._run_local(context=context, base_spec=base_spec, extras=extras)
        else:
            # Run on Kubernetes with Spark Operator
            return self._run_kubernetes(
                context=context,
                base_spec=base_spec,
                namespace=namespace,
                extras=extras,
                cleanup=cleanup,
                driver_resource=driver_resource,
                executor_resource=executor_resource,
            )

    @classmethod
    def _file_path_to_module(self, file_path: str) -> str:
        """Convert file path to Python module path.

        Examples:
            /app/src/transform/raw_tss/main.py -> src.transform.raw_tss.main
            local:///app/src/transform/main.py -> src.transform.main
        """
        # Remove local:// prefix if present
        if file_path.startswith("local://"):
            file_path = file_path[8:]

        # Remove /app/ prefix if present (Docker path convention)
        if file_path.startswith("/app/"):
            file_path = file_path[5:]

        # Remove leading slash
        file_path = file_path.lstrip("/")

        # Remove .py extension
        if file_path.endswith(".py"):
            file_path = file_path[:-3]

        # Convert path separators to dots
        return file_path.replace("/", ".")

    def _run_local(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        base_spec: dict[str, Any],
        extras: dict[str, Any] | None = None,
    ) -> PipesClientCompletedInvocation:
        """Run Spark job locally by calling its Click CLI with the provided arguments.

        This method works with any Spark job that has a Click CLI (e.g., BaseSpark subclasses).
        The job manages its own Spark session creation.

        Args:
            context: Dagster execution context
            base_spec: Must include 'mainApplicationFile' and 'arguments'
            extras: Additional Pipes extras

        Example:
            base_spec = {
                "mainApplicationFile": "local:///app/src/transform/raw_tss/main.py",
                "arguments": ["run", "bmw"]  # or ["run", "--all"]
            }
        """
        # Extract mainApplicationFile and arguments
        main_file = base_spec.get("mainApplicationFile", "")
        arguments = base_spec.get("arguments", [])

        if not main_file:
            raise ValueError(
                "base_spec must include 'mainApplicationFile' for local execution"
            )

        # Convert file path to module path
        module_path = self._file_path_to_module(main_file)
        context.log.info(f"Running Spark job locally: {module_path}")
        context.log.info(f"Arguments: {arguments}")

        with open_pipes_session(
            context=context,
            extras=extras,
            context_injector=self.context_injector,
            message_reader=PipesTempFileMessageReader(include_stdio_in_messages=True),
        ) as pipes_session:
            # Set up environment variables for Pipes context
            env_vars = pipes_session.get_bootstrap_env_vars()
            original_env = {}
            for key, value in env_vars.items():
                original_env[key] = os.environ.get(key)
                os.environ[key] = value

            # Save original sys.argv
            original_argv = sys.argv.copy()

            try:
                # Import the module
                context.log.info(f"Importing module: {module_path}")
                module = importlib.import_module(module_path)

                # Look for a Click CLI (BaseSpark subclasses expose this)
                if not hasattr(module, "cli"):
                    raise ValueError(
                        f"No 'cli' (Click) found in {module_path}. "
                        "Spark jobs must expose a Click CLI entry point."
                    )

                # Invoke Click command with arguments
                context.log.info("Found Click CLI, invoking with arguments...")
                sys.argv = [main_file, *arguments]

                # Invoke the CLI (standalone_mode=False prevents sys.exit)
                try:
                    module.cli(standalone_mode=False)
                    context.log.info("Click CLI executed successfully")
                except SystemExit as e:
                    if e.code != 0:
                        raise RuntimeError(f"CLI exited with code {e.code}") from e
                    context.log.info("Click CLI completed")

                context.log.info(f"Local Spark job completed: {module_path}")

                return PipesClientCompletedInvocation(
                    pipes_session,
                    metadata={
                        "execution_mode": "local",
                        "module": module_path,
                        "arguments": " ".join(arguments),
                    },
                )

            finally:
                # Restore sys.argv
                sys.argv = original_argv

                # Clean up environment variables (restore originals)
                for key in env_vars:
                    if original_env.get(key) is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = original_env[key]

    def _run_kubernetes(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        base_spec: dict[str, Any],
        namespace: str | None = None,
        extras: dict[str, Any] | None = None,
        cleanup: bool = True,
        driver_resource: DriverResource | None = None,
        executor_resource: ExecutorResource | None = None,
    ) -> PipesClientCompletedInvocation:
        """Run Spark job on Kubernetes via Spark Operator."""
        self._load_k8s_config()
        custom_objects_api = kubernetes.client.CustomObjectsApi()
        core_api = kubernetes.client.CoreV1Api()

        with open_pipes_session(
            context=context,
            extras=extras,
            context_injector=self.context_injector,
            message_reader=self.message_reader,
        ) as pipes_session:
            namespace = (
                namespace or detect_current_namespace(self.kubeconfig_file) or "default"
            )
            app_name = get_spark_app_name(context)

            # Enrich spec with Pipes context
            enriched_spec = self._enrich_spark_app_spec(
                base_spec,
                pipes_session.get_bootstrap_env_vars(),
            )

            if driver_resource:
                enriched_spec["driver"] |= driver_resource.model_dump()
            if executor_resource:
                enriched_spec["executor"] |= executor_resource.model_dump()

            # Build SparkApplication manifest
            spark_app = {
                "apiVersion": "sparkoperator.k8s.io/v1beta2",
                "kind": "SparkApplication",
                "metadata": {
                    "name": app_name,
                    "namespace": namespace,
                    "labels": {
                        "dagster/run-id": context.run_id,
                        "dagster/op-name": context.op.name,
                    },
                },
                "spec": enriched_spec,
            }

            try:
                # Create SparkApplication
                context.log.info(
                    f"Creating SparkApplication {app_name} in namespace {namespace}"
                )
                spark_app_id = self._create_spark_application_with_retry(
                    context=context,
                    custom_objects_api=custom_objects_api,
                    core_api=core_api,
                    namespace=namespace,
                    spark_app=spark_app,
                )

                # Wait for completion and consume logs
                driver_pod_name = f"{app_name}-driver"

                # Start log consumption in background if message reader supports it
                if isinstance(
                    self.message_reader, PipesSparkApplicationLogsMessageReader
                ):
                    # Wait for driver pod to be created by the SparkApplication
                    self._wait_for_driver_pod_creation(
                        context=context,
                        core_api=core_api,
                        driver_pod_name=driver_pod_name,
                        namespace=namespace,
                    )
                    self.message_reader.consume_driver_logs(
                        context=context,
                        core_api=core_api,
                        driver_pod_name=driver_pod_name,
                        namespace=namespace,
                    )

                # Wait for SparkApplication to complete
                final_status = self._wait_for_completion(
                    context=context,
                    custom_objects_api=custom_objects_api,
                    app_name=app_name,
                    namespace=namespace,
                )

                context.log.info(
                    f"SparkApplication {app_name} completed with state: {final_status}"
                )

                metadata = {
                    "spark_application_name": app_name,
                    "namespace": namespace,
                    "final_state": final_status,
                }

                if spark_app_id:
                    metadata["spark_history_url"] = SPARK_HISTORY_URL_TEMPLATE.format(
                        spark_app_id=spark_app_id
                    )

                return PipesClientCompletedInvocation(
                    pipes_session,
                    metadata=metadata,
                )

            except DagsterExecutionInterruptedError:
                if self.forward_termination and cleanup:
                    context.log.warning(
                        f"Dagster execution interrupted. Deleting SparkApplication {app_name}"
                    )
                    self._terminate(
                        context=context,
                        custom_objects_api=custom_objects_api,
                        app_name=app_name,
                        namespace=namespace,
                    )
                raise
            finally:
                # Cleanup: delete the SparkApplication
                if cleanup:
                    self._terminate(
                        context=context,
                        custom_objects_api=custom_objects_api,
                        app_name=app_name,
                        namespace=namespace,
                    )

    def _check_submission_status(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        custom_objects_api: kubernetes.client.CustomObjectsApi,
        app_name: str,
        namespace: str,
        submission_check_timeout: int = 500,
        poll_interval: float = 2.0,
    ) -> bool:
        """Poll SparkApplication until submission succeeds or fails.

        Returns True if submission succeeds, False if it fails (for retry).
        Raises on timeout or unexpected errors.
        """
        context.log.info(
            f"Polling SparkApplication {app_name} status (timeout: {submission_check_timeout}s)..."
        )
        start_time = time.time()

        while time.time() - start_time < submission_check_timeout:
            try:
                app = custom_objects_api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=namespace,
                    plural="sparkapplications",
                    name=app_name,
                )

                status = app.get("status", {})
                app_state = status.get("applicationState", {}).get("state", "")

                # If no state yet, keep polling
                if not app_state:
                    time.sleep(poll_interval)
                    continue

                # Check for failure states
                if app_state in ["SUBMISSION_FAILED", "FAILED"]:
                    error_message = status.get("applicationState", {}).get(
                        "errorMessage", "No error message"
                    )
                    context.log.warning(
                        f"SparkApplication {app_name} entered {app_state} state: {error_message}"
                    )
                    return False

                # Any other state means submission succeeded
                context.log.info(
                    f"SparkApplication {app_name} submission successful (state: {app_state})"
                )
                return True

            except kubernetes.client.exceptions.ApiException as exc:
                if exc.status == 404:
                    raise RuntimeError(
                        f"SparkApplication {app_name} was deleted unexpectedly"
                    ) from exc
                raise

        # Timeout reached
        raise TimeoutError(
            f"SparkApplication {app_name} submission status check timed out after {submission_check_timeout}s"
        )

    def _delete_failed_spark_application(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        custom_objects_api: kubernetes.client.CustomObjectsApi,
        core_api: kubernetes.client.CoreV1Api,
        app_name: str,
        namespace: str,
        wait_for_cleanup: int = 60,
    ) -> None:
        """Delete failed SparkApplication and wait for driver pod cleanup.

        Waits up to wait_for_cleanup seconds for the driver pod to be deleted
        before proceeding. Logs warning but doesn't fail if cleanup times out.
        """
        try:
            custom_objects_api.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=app_name,
            )
            context.log.info(f"Deleted failed SparkApplication {app_name}")

            # Wait for driver pod to be cleaned up
            driver_pod_name = f"{app_name}-driver"
            context.log.info(
                f"Waiting up to {wait_for_cleanup}s for driver pod {driver_pod_name} to be deleted..."
            )
            start_time = time.time()

            while time.time() - start_time < wait_for_cleanup:
                try:
                    core_api.read_namespaced_pod(driver_pod_name, namespace)
                    # Pod still exists, keep waiting
                    time.sleep(2)
                except kubernetes.client.ApiException as e:
                    if e.status == 404:
                        # Pod deleted, we're good
                        elapsed = time.time() - start_time
                        context.log.info(
                            f"Driver pod {driver_pod_name} cleaned up after {elapsed:.1f}s"
                        )
                        return
                    raise

            # Timeout - log warning but don't fail
            context.log.warning(
                f"Driver pod {driver_pod_name} still exists after {wait_for_cleanup}s. "
                "Proceeding with retry anyway."
            )

        except kubernetes.client.exceptions.ApiException as exc:
            if exc.status != 404:
                context.log.warning(
                    f"Failed to delete SparkApplication {app_name}: {exc}"
                )

    def _create_spark_application_with_retry(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        custom_objects_api: kubernetes.client.CustomObjectsApi,
        core_api: kubernetes.client.CoreV1Api,
        namespace: str,
        spark_app: dict[str, Any],
        max_retries: int = 3,
        retry_delay: int = 30,
    ) -> str | None:
        """Create SparkApplication with retry logic for transient failures.

        Retries on API errors (500s) and submission failures (SUBMISSION_FAILED).
        Waits for full cleanup between retries to avoid "driver pod already exists" errors.

        Returns spark_app_id if available, None otherwise.
        """
        app_name = spark_app["metadata"]["name"]

        for attempt in range(max_retries):
            try:
                # Create the SparkApplication
                custom_objects_api.create_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=namespace,
                    plural="sparkapplications",
                    body=spark_app,
                )
                context.log.info(f"SparkApplication {app_name} created successfully")

                # Check if submission succeeded
                submission_succeeded = self._check_submission_status(
                    context=context,
                    custom_objects_api=custom_objects_api,
                    app_name=app_name,
                    namespace=namespace,
                )

                if submission_succeeded:
                    return self._wait_for_spark_application_id(
                        context=context,
                        custom_objects_api=custom_objects_api,
                        app_name=app_name,
                        namespace=namespace,
                    )

                # Submission failed, retry if attempts remain
                if attempt < max_retries - 1:
                    context.log.warning(
                        f"Retrying SparkApplication creation (attempt {attempt + 2}/{max_retries})..."
                    )
                    self._delete_failed_spark_application(
                        context=context,
                        custom_objects_api=custom_objects_api,
                        core_api=core_api,
                        app_name=app_name,
                        namespace=namespace,
                    )
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(
                        f"SparkApplication {app_name} failed to submit after {max_retries} attempts"
                    )

            except kubernetes.client.exceptions.ApiException as exc:
                if exc.status == 500 and attempt < max_retries - 1:
                    context.log.warning(
                        f"API error creating SparkApplication (attempt {attempt + 1}/{max_retries}): {exc.reason}. "
                        f"Retrying in {retry_delay} seconds..."
                    )
                    time.sleep(retry_delay)
                else:
                    raise

    def _wait_for_spark_application_id(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        custom_objects_api: kubernetes.client.CustomObjectsApi,
        app_name: str,
        namespace: str,
        max_wait: int = 30,
        poll_interval: float = 2.0,
    ) -> str | None:
        """Poll for sparkApplicationId and log Spark History URL when available."""
        context.log.debug(
            "Submission succeeded but spark_app_id not yet available. Polling for ID..."
        )
        start = time.time()

        while time.time() - start < max_wait:
            try:
                app = custom_objects_api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=namespace,
                    plural="sparkapplications",
                    name=app_name,
                )
            except kubernetes.client.exceptions.ApiException:
                return None  # Don't fail if we can't get the ID

            spark_app_id = app.get("status", {}).get("sparkApplicationId")
            if spark_app_id:
                context.log.info(
                    f"Spark History Server: {SPARK_HISTORY_URL_TEMPLATE.format(spark_app_id=spark_app_id)}"
                )
                return spark_app_id

            time.sleep(poll_interval)

        context.log.warning(
            f"Cannot get SparkApplication ID after {max_wait} seconds for SparkApplication {app_name}."
        )
        return None

    def _wait_for_driver_pod_creation(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        core_api: kubernetes.client.CoreV1Api,
        driver_pod_name: str,
        namespace: str,
        max_wait: int = DEFAULT_MAX_WAIT,
    ) -> None:
        """Wait for driver pod to be created by SparkApplication.

        Raises TimeoutError if pod not created within max_wait seconds.
        """
        context.log.info(
            f"Waiting for driver pod {driver_pod_name} to be created by SparkApplication..."
        )
        start_time = time.time()

        while time.time() - start_time < max_wait:
            try:
                # Try to read the pod - if it exists, we're done
                core_api.read_namespaced_pod(driver_pod_name, namespace)
                elapsed = time.time() - start_time
                context.log.info(
                    f"Driver pod {driver_pod_name} created after {elapsed:.1f} seconds"
                )
                return
            except kubernetes.client.ApiException as e:
                if e.status == 404:
                    # Pod doesn't exist yet, keep waiting
                    time.sleep(DEFAULT_POLL_INTERVAL)
                    continue
                else:
                    # Some other API error occurred
                    raise

        # Timeout reached
        elapsed = time.time() - start_time
        raise TimeoutError(
            f"Driver pod {driver_pod_name} was not created within {max_wait} seconds (waited {elapsed:.1f}s)"
        )

    def _wait_for_completion(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        custom_objects_api: kubernetes.client.CustomObjectsApi,
        app_name: str,
        namespace: str,
    ) -> str:
        """Wait for SparkApplication to complete and return final state."""
        context.log.info(f"Waiting for SparkApplication {app_name} to complete...")

        max_attempts = int(DEFAULT_WAIT_TIMEOUT / DEFAULT_POLL_INTERVAL)
        attempt = 0

        while attempt < max_attempts:
            try:
                app = custom_objects_api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=namespace,
                    plural="sparkapplications",
                    name=app_name,
                )

                status = app.get("status", {})
                app_state = status.get("applicationState", {}).get("state", "UNKNOWN")

                context.log.debug(f"SparkApplication state: {app_state}")

                # Terminal states
                if app_state in [
                    "COMPLETED",
                    "FAILED",
                    "SUBMISSION_FAILED",
                    "INVALIDATING",
                ]:
                    if app_state in ["FAILED", "SUBMISSION_FAILED", "INVALIDATING"]:
                        error_message = status.get("applicationState", {}).get(
                            "errorMessage", "No error message"
                        )
                        raise RuntimeError(
                            f"SparkApplication {app_name} failed with state {app_state}: {error_message}"
                        )
                    return app_state

            except kubernetes.client.ApiException as e:
                if e.status == 404:
                    context.log.warning(f"SparkApplication {app_name} not found")
                    raise RuntimeError(
                        f"SparkApplication {app_name} was deleted unexpectedly"
                    ) from e
                raise

            time.sleep(DEFAULT_POLL_INTERVAL)
            attempt += 1

        raise TimeoutError(
            f"SparkApplication {app_name} did not complete within {DEFAULT_WAIT_TIMEOUT} seconds"
        )

    def _terminate(
        self,
        context: OpExecutionContext | AssetExecutionContext,
        custom_objects_api: kubernetes.client.CustomObjectsApi,
        app_name: str,
        namespace: str,
    ):
        """Terminate a running SparkApplication."""
        try:
            context.log.info(f"Terminating SparkApplication {app_name}")
            custom_objects_api.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
                name=app_name,
            )
            context.log.info(f"SparkApplication {app_name} terminated")
        except kubernetes.client.ApiException as e:
            if e.status != 404:
                context.log.error(f"Failed to terminate SparkApplication: {e}")
