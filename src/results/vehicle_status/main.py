import logging

from src.results.vehicle_status.vehicle_status_tracker import VehicleStatusTracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_vehicle_status_checks(logger: logging.Logger = logger):
    """Run all vehicle status checks and return a summary."""
    tracker = VehicleStatusTracker(logger)
    tracker.run_all_checks()
    return {"status": "completed", "message": "All vehicle status checks completed"}


if __name__ == "__main__":
    run_vehicle_status_checks()
