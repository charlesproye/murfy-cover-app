"""Spark jobs using PipesSparkApplicationClient."""

from dagster import StaticPartitionsDefinition

from core.models.make import MakeEnum

MAKE_PARTITIONS = StaticPartitionsDefinition([make.value for make in MakeEnum])
