"""
Tesla Fleet Telemetry Module

This module implements the ingestion and storage of telemetry data from Tesla vehicles
via a Kafka stream. Data is stored in S3 with a compression mechanism to optimize storage.

Features:
- Asynchronous Kafka consumption
- Efficient S3 storage with temp/compression mechanism
- Data processing and normalization
- Automatic data cleanup based on retention policy
"""

__version__ = "1.0.0"

