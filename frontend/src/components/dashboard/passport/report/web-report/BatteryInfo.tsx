import { IconBatteryCharging } from '@tabler/icons-react';
import React from 'react';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';

interface BatteryInfoProps {
  vehicleBatteryInfo: InfoVehicleResult;
}

const BatteryInfo: React.FC<BatteryInfoProps> = ({ vehicleBatteryInfo }) => {
  return (
    <div className="bg-white rounded-xl p-4 md:p-6 mb-6 shadow-sm max-w-4xl w-full">
      <div className="flex items-center space-x-2 mb-4">
        <div className="w-6 h-6 rounded-full bg-green-100 flex items-center justify-center">
          <IconBatteryCharging className="w-16 h-16 text-green-rapport" />
        </div>
        <h3 className="font-medium text-lg">Battery Information</h3>
      </div>

      <div className="flex justify-between gap-2 mb-4 w-full">
        <div className="flex-1 p-2  rounded-lg">
          <p className="text-gray text-sm">BATTERY OEM</p>
          <p className="font-medium text-sm truncate">
            {vehicleBatteryInfo.battery_info?.oem || 'Unknown'}
          </p>
        </div>

        <div className="flex-1 p-2 rounded-lg">
          <p className="text-gray text-sm">CHEMISTRY</p>
          <p className="font-medium text-sm truncate">
            {vehicleBatteryInfo.battery_info?.chemistry || 'Unknown'}
          </p>
        </div>

        <div className="flex-1 p-2 rounded-lg">
          <p className="text-gray text-sm">CAPACITY</p>
          <p className="font-medium text-sm truncate">
            {vehicleBatteryInfo.battery_info?.capacity === 0
              ? ''
              : `${vehicleBatteryInfo.battery_info?.capacity} kWh`}
          </p>
        </div>

        <div className="flex-1 p-2 rounded-lg">
          <p className="text-gray text-sm">WLTP RANGE</p>
          <p className="font-medium text-sm truncate">
            {vehicleBatteryInfo.battery_info?.range === 0
              ? 'Unknown'
              : `${vehicleBatteryInfo.battery_info?.range} km`}
          </p>
        </div>

        <div className="flex-1 p-2 rounded-lg">
          <p className="text-gray text-sm">CONSUMPTION</p>
          <p className="font-medium text-sm truncate">
            {vehicleBatteryInfo.battery_info?.consumption === 0
              ? ''
              : `${vehicleBatteryInfo.battery_info?.consumption} kWh/100km`}
          </p>
        </div>
      </div>

      <div className="flex flex-wrap md:flex-nowrap gap-1">
        <div className="w-full overflow-hidden border border-green-rapport-light rounded-lg shadow-sm flex flex-col md:flex-row">
          <div className="bg-green-rapport-light p-4 flex flex-col justify-center items-center md:w-1/4">
            <p className="text-white text-xs uppercase font-medium">
              State of Health (SoH)
            </p>
            <div className="relative flex items-center justify-center">
              <span className="text-2xl font-bold text-white">
                {vehicleBatteryInfo.battery_info?.soh}%
              </span>
            </div>
          </div>

          <div className="bg-green-rapport-light/10 px-4 md:w-3/4 flex flex-col justify-center">
            <p className="text-sm">
              The SoH is calculated by Bib based on raw driving data reported by the
              vehicle's BMS and stored in the manufacturer's cloud. Bib provides an
              independent SoH, which does not imply any manufacturer liability.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default BatteryInfo;
