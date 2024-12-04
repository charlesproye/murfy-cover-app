from datetime import datetime
from typing import Optional, List
from ingestion.bmw import MergedInfoProtocol
import msgspec
import logging

class BMWDataPoint(msgspec.Struct):
    key: str
    value: Optional[str]
    unit: Optional[str]
    info: Optional[str]
    date_of_value: Optional[datetime]

class BMWInfo(msgspec.Struct):
    data: List[BMWDataPoint]

class BMWMergedInfo(MergedInfoProtocol[BMWInfo]):
    def __init__(self):
        self.data_points: List[BMWDataPoint] = []
        self.fusion_stats = {"new": 0, "updated": 0, "unchanged": 0}
        self.logger = logging.getLogger(__name__)

    @classmethod
    def new(cls) -> 'BMWMergedInfo':
        return cls()

    @classmethod
    def from_initial(cls, initial: BMWInfo) -> 'BMWMergedInfo':
        instance = cls()
        instance.data_points.extend(initial.data)
        instance.fusion_stats["new"] = len(initial.data)
        return instance

    def merge(self, other: BMWInfo) -> None:
        for point in other.data:
            existing_points = [p for p in self.data_points if p.key == point.key]
            if not existing_points:
                self.data_points.append(point)
                self.fusion_stats["new"] += 1
                self.logger.debug(f"New data point added: {point.key}")
            else:
                updated = False
                for existing_point in existing_points:
                    if point.date_of_value and (not existing_point.date_of_value or point.date_of_value > existing_point.date_of_value):
                        self.data_points.remove(existing_point)
                        self.data_points.append(point)
                        self.fusion_stats["updated"] += 1
                        updated = True
                        self.logger.debug(f"Data point updated: {point.key}")
                        break
                    elif point.date_of_value == existing_point.date_of_value:
                        if point.value != existing_point.value:
                            if point.value is not None:
                                self.data_points.remove(existing_point)
                                self.data_points.append(point)
                                self.fusion_stats["updated"] += 1
                                updated = True
                                break
                        else:
                            self.fusion_stats["unchanged"] += 1
                            updated = True
                            break
                if not updated:
                    self.data_points.append(point)
                    self.fusion_stats["new"] += 1

    def to_bmw_info(self) -> BMWInfo:
        return BMWInfo(data=self.data_points)

    def get_fusion_stats(self) -> str:
        return f"Fusion stats: {self.fusion_stats}"
