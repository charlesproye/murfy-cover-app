from pydantic import BaseModel, computed_field


class PodStatus(BaseModel):
    pod_name: str
    namespace: str
    status: str
    error_message: str | None = None

    @computed_field
    @property
    def is_ready(self) -> bool:
        return self.status in {"Running", "Succeeded", "Failed"}
