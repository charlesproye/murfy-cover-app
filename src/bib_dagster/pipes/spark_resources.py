from pydantic import BaseModel


class DriverResource(BaseModel):
    cores: int
    memory: str
    memoryOverhead: str


class ExecutorResource(BaseModel):
    cores: int
    instances: int
    memory: str
    memoryOverhead: str
