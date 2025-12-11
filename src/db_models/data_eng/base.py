"""Declarative base for the data-engineering database."""

from sqlalchemy.orm import declarative_base

DataEngBase = declarative_base()

__all__ = ["DataEngBase"]
