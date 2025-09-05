from transform.compressor.providers.bmw import BMWCompressor
from transform.compressor.providers.mobilisight import MobilisightCompressor
from transform.compressor.providers.tesla_fleet_telemetry import TeslaFTCompressor
from transform.compressor.providers.volkswagen import VolkswagenCompressor
from transform.compressor.providers.high_mobility import HighMobilityCompressor
from core.console_utils import main_decorator

COMPRESSORS= {
    "bmw": BMWCompressor,
    "mercedes-benz": HighMobilityCompressor,
    "renault": HighMobilityCompressor,
    "volvo-cars": HighMobilityCompressor,
    "stellantis": MobilisightCompressor,
    "kia": HighMobilityCompressor,
    "ford": HighMobilityCompressor,
    "tesla-fleet-telemetry": TeslaFTCompressor,
    "volkswagen": VolkswagenCompressor
}
