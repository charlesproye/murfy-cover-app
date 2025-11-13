import os
from abc import ABC, abstractmethod

import click
from pyspark.sql.session import SparkSession

from core import spark_utils
from core.s3.settings import S3Settings


class BaseSpark(ABC):
    """Base class enforcing a run() contract and click setup."""

    def __init__(self, name: str, help: str | None = None):
        self.name = name
        self.help = help or f"{name} command group"
        self.cli: click.Group = click.Group(name=self.name, help=self.help)
        self._register_commands()
        self._spark: SparkSession | None = None

    @classmethod
    def file_path_in_docker(cls) -> str:
        module_path = cls.__module__.replace(".", "/")
        return "local:///app/src/" + module_path + ".py"

    @property
    def spark(self) -> SparkSession:
        if self._spark is not None:
            return self._spark

        settings = S3Settings()
        is_k8s = os.getenv("KUBERNETES_SERVICE_HOST") is not None

        if is_k8s:
            spark = spark_utils.create_spark_session_k8s(
                settings.S3_KEY, settings.S3_SECRET
            )
        else:
            spark = spark_utils.create_spark_session(
                settings.S3_KEY, settings.S3_SECRET
            )

        self._spark = spark
        return spark

    @abstractmethod
    def run(self, **kwargs):
        """Each subclass must implement `run`."""

    def _register_commands(self):
        """Automatically register run() + any additional subcommands."""

        # Create a wrapper function that properly binds self
        def run_wrapper(*args, **kwargs):
            return self.run(*args, **kwargs)

        # Apply the run method's decorators to the wrapper
        run_wrapper.__name__ = self.run.__name__
        run_wrapper.__doc__ = self.run.__doc__

        # Copy any click parameters from the original method
        if hasattr(self.run, "__click_params__"):
            run_wrapper.__click_params__ = self.run.__click_params__

        self.cli.command("run")(run_wrapper)

        # Subclasses can override `add_subcommands` to register more
        self.add_subcommands()

    def add_subcommands(self):
        """Override to register additional click commands."""
        return
