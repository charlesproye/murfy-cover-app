"""This is a Work In Progress"""

from .client import VolksWagenClient
from .schemas import VehicleConfirmation


class Controller:
    def __init__(self) -> None:
        self._vw_client = VolksWagenClient()

    # How to log a vehicle
    async def log_vehicle(self, vin: str):
        r = await self._vw_client.post_vehicle_approval(["vins"])
        code = r.json()[0]  # return a list of codes that is use to confirm the vehicle
        confirmation = VehicleConfirmation(
            vin=vin, verification_code=code["vehicle-verification-codes"]
        )
        r = await self._vw_client.post_vehicle_confirmation([confirmation])
        return

