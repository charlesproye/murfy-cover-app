from collections import defaultdict
import asyncio
import time
import logging
from datetime import datetime, timedelta

class VehiclePool:
    def __init__(self, size=100):
        self.vehicles = {}
        self.size = size
        self.current_index = 0
        self.last_success = defaultdict(lambda: None)
        self.sleep_until = defaultdict(lambda: None)
        self.failed_attempts = defaultdict(int)
        self.wake_up_tasks = {}
        self.processing = set()

    async def process_wake_up(self, vehicle_id, access_token):
        """Gère le réveil d'un véhicule de manière asynchrone"""
        if vehicle_id in self.wake_up_tasks:
            return
            
        self.wake_up_tasks[vehicle_id] = asyncio.create_task(
            wake_up_vehicle(access_token, vehicle_id)
        )
        try:
            await self.wake_up_tasks[vehicle_id]
        finally:
            del self.wake_up_tasks[vehicle_id]

    def add_vehicle(self, vehicle):
        self.vehicles[vehicle['id']] = vehicle

    def get_next_batch(self, current_time):
        """Get next batch of vehicles that are ready to be processed"""
        ready_vehicles = []
        checked = 0
        vehicle_ids = list(self.vehicles.keys())
        
        while checked < len(vehicle_ids) and len(ready_vehicles) < self.size:
            idx = self.current_index % len(vehicle_ids)
            vehicle_id = vehicle_ids[idx]
            
            if (self._is_vehicle_ready(vehicle_id, current_time) and 
                vehicle_id not in self.processing):
                ready_vehicles.append(self.vehicles[vehicle_id])
                self.processing.add(vehicle_id)
            
            self.current_index = (self.current_index + 1) % len(vehicle_ids)
            checked += 1
            
        return ready_vehicles

    def release_vehicle(self, vehicle_id):
        """Libère un véhicule après son traitement"""
        if vehicle_id in self.processing:
            self.processing.remove(vehicle_id)

    def _is_vehicle_ready(self, vehicle_id, current_time):
        """Vérifie si un véhicule est prêt à être traité"""
        if self.sleep_until.get(vehicle_id) and current_time < self.sleep_until[vehicle_id]:
            return False

        last_success = self.last_success.get(vehicle_id)
        if last_success and (current_time - last_success).total_seconds() < 300:
            logging.debug(f"Vehicle {vehicle_id} not ready yet. Last success: {last_success}")
            return False

        return True

    def update_vehicle_status(self, vehicle_id, success, is_sleeping=False):
        current_time = datetime.now()
        
        if success:
            self.last_success[vehicle_id] = current_time
            self.failed_attempts[vehicle_id] = 0
            if is_sleeping:
                self.sleep_until[vehicle_id] = current_time + timedelta(minutes=30)
        else:
            self.failed_attempts[vehicle_id] += 1
            if self.failed_attempts[vehicle_id] > 3:
                delay_minutes = min(2 ** (self.failed_attempts[vehicle_id] - 3), 60)
                self.sleep_until[vehicle_id] = current_time + timedelta(minutes=delay_minutes)
